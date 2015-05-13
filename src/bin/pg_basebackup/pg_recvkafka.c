#include "postgres_fe.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "streamutil.h"
#include "access/xlog_internal.h"
#include "common/fe_memutils.h"
#include "getopt_long.h"
#include "libpq-fe.h"
#include "libpq/pqsignal.h"
#include "pqexpbuffer.h"
#include "kafka.h"

/* pg requires that we send keepalives, it also requests us to send them,
 * these variables keep track of how often we keepalive and the last time we
 * did */
static int64 last_keepalive_us = 0;
static int64 keepalive_interval_us = 10000000; /* 10 secs */

/* some std cmd line parameters */
static int	verbose = 0;
static char **options;
static size_t noptions = 0;

/* we need to track where we're at in the wal log, and what's been "fsync'd"
 * (aka durably written) to kafka -- that's what these do */
static XLogRecPtr output_start_lsn = InvalidXLogRecPtr;
static XLogRecPtr output_fsync_lsn = InvalidXLogRecPtr;
static XLogRecPtr output_written_lsn = InvalidXLogRecPtr;

/* flag ctrl-c's with this */
static volatile sig_atomic_t time_to_abort = false;

/* if we have an error, we kill the process for safety
 * this is where we do _some_ cleanup for that */
static void
fail_fast()
{
	kafka_destroy();

	if (conn != NULL)
		PQfinish(conn);

	exit(1);
}

static void
send_keepalive(bool force)
{
	/* constant now() so server and client timestamps equal */
	int64 now = feGetCurrentTimestamp();

	/* store these locally for a couple optimizations below */
	static XLogRecPtr last_written_lsn = InvalidXLogRecPtr;
	static XLogRecPtr last_fsync_lsn = InvalidXLogRecPtr;

	/* http://www.postgresql.org/docs/9.4/static/protocol-replication.html */
	char replybuf[1 + 8 + 8 + 8 + 8 + 1];
	int len = 0;

	/* we normally don't want to send superfluous feedbacks, but if it's
	 * because of a timeout we need to, otherwise wal_sender_timeout will kill
	 * us. */
	if (!force &&
		last_written_lsn == output_written_lsn &&
		last_fsync_lsn != output_fsync_lsn)
		return;

	debugf("Confirming write up to %X/%X, flush to %X/%X (slot %s)\n",
		   (uint32) (output_written_lsn >> 32), (uint32) output_written_lsn,
		   (uint32) (output_fsync_lsn >> 32), (uint32) output_fsync_lsn,
		   replication_slot);

	/* build up our status feedback buffer */
	replybuf[len] = 'r';
	len += 1;
	fe_sendint64(output_written_lsn, &replybuf[len]); /* write */
	len += 8;
	fe_sendint64(output_fsync_lsn, &replybuf[len]); /* flush */
	len += 8;
	fe_sendint64(InvalidXLogRecPtr, &replybuf[len]); /* apply */
	len += 8;
	fe_sendint64(now, &replybuf[len]); /* sendTime */
	len += 8;
	replybuf[len] = false; /* replyRequested */
	len += 1;

	/* track the lsns we told the server about */
	output_start_lsn = output_written_lsn;
	last_written_lsn = output_written_lsn;
	last_fsync_lsn = output_fsync_lsn;

	/* make sure everything made it to server ok */
	if (PQputCopyData(conn, replybuf, len) <= 0 || PQflush(conn))
	{
		errorf("Could not send feedback packet: %s", PQerrorMessage(conn));
		fail_fast();
	}

	/* record that we sent a keepalive now */
	last_keepalive_us = now;
}

static void
maybe_send_keepalive(void)
{
	/* if we're past our keepalive interval, send a keepalive */
	if ((feGetCurrentTimestamp() - last_keepalive_us) >= keepalive_interval_us)
	{
		/* send it */
		send_keepalive(true);
	}
}

static void
handle_keepalive(void *copybuf, size_t copylen)
{
	/* the server may want a reply */
	bool replyRequested;

	/* the end of the wal on the server */
	XLogRecPtr walEnd;

	/* make sure we have a keepalive payload */
	if (copybuf == NULL)
	{
		errorf("Empty keepalive received\n");
		fail_fast();
	}

	/* double-check that the size is correct */
	if (copylen < 18)
	{
		errorf("Streaming header too small: %zu\n", copylen);
		fail_fast();
	}

	/* grab the wal end position */
	walEnd = fe_recvint64(&((char*) copybuf)[1]);

	/* TODO check walEnd against our output_written_lsn to check if we're
	 * behind or whatever */

	/* read if the server wants a reply */
	replyRequested = ((char*) copybuf)[17];

	/* if the server requested an immediate reply, send one. */
	if (replyRequested)
	{
		/* tell the server our status */
		send_keepalive(true);
	}
}

static void
kafka_checkpoint_callback(int err, void *checkpoint, size_t checkpointlen)
{
	/* check if we had a delivery failure */
	if (err)
	{
		errorf("Received %d sending checkpoint to kafka\n", err);
		fail_fast();
	}

	/* double-check that the size is correct */
	if (checkpointlen != 9)
	{
		errorf("Checkpoint payload too small: %zu\n", checkpointlen);
		fail_fast();
	}

	/* all we care about is that the checkpoint was written, so free */
	free(checkpoint);
}

static void
kafka_checkpoint(void)
{
	int ret;

	/* keep track of the last checkpoint we sent */
	static XLogRecPtr last_checkpoint_lsn = InvalidXLogRecPtr;

	/* we're treating this kafka stream as a write ahead log
	 * so checkpoint it at the last known successful lsn */
	char *checkpoint;

	/* only send checkpoints if the "fsync'd" lsn has changed */
	if (last_checkpoint_lsn >= output_fsync_lsn)
	{
		return;
	}

	/* use the format 'C########') */
	checkpoint = malloc(9);
	checkpoint[0] = 'C';
	fe_sendint64(output_fsync_lsn, checkpoint + 1);

	/* try to send the checkpoint to kafka */
	ret = kafka_send_msg(checkpoint, 1,
						 checkpoint + 1, 8, /* lsn */
						 checkpoint, 9);
	if (ret < 0)
	{
		errorf("Could not send checkpoint to kafka: %d\n", ret);
		fail_fast();
	}

	/* record that we attempted to send this checkpoint */
	last_checkpoint_lsn = output_fsync_lsn;
}

static void
kafka_callback(int err, void *copybuf, size_t copylen)
{
	/* check if we had a delivery failure */
	if (err)
	{
		errorf("Received %d sending to kafka\n", err);
		fail_fast();
	}

	/* make sure we have a callback payload */
	if (copybuf == NULL)
	{
		errorf("Empty callback received\n");
		fail_fast();
	}

	/* double-check that the size is correct
	 * 25 == msgtype 'w' + dataStart + walEnd + sendTime */
	if (copylen <= 25)
	{
		errorf("Callback header too small: %zu\n", copylen);
		fail_fast();
	}

	/* double-check that we have the correct message type */
	if (((char*) copybuf)[0] != 'w')
	{
		errorf("Unrecognized callback header: \"%c\"\n", ((char*) copybuf)[0]);
		fail_fast();
	}

	/* record that we "fsync'd" this lsn to kafka */
	output_fsync_lsn = fe_recvint64(&((char *) copybuf)[1]);

	/* and cleanup the now unused mem */
	PQfreemem(copybuf);
}

static void
kafka_read_msg_delivery_pipe(void)
{
	/* the kafka lib writes delivered/failed messages to a pipe
	 * this is where we read them */

	/* the kafka message we're currently processing */
	kafka_msg *msg = NULL;

	/* keep track of how many messages we've read */
	int msg_count = 0;

	/* number of bytes to read from pipe per msg */
	int bytes_to_read = sizeof(msg);

	/* keep track of how much data we've read per read */
	int bytes_read = 0;

	/* keep track of how much data we've read per msg */
	int bytes_total_read = 0;

	/* read messages off the pipe until there's nothing left */
	while ((bytes_read = read(kafka_msg_delivery_pipe,
							  &msg + bytes_total_read,
							  bytes_to_read - bytes_total_read)) > 0)
	{
		/* keep track of how much we've read in total for this msg */
		bytes_total_read += bytes_read;

		/* ensure we've read the entire msg, or read more */
		if (bytes_total_read < bytes_to_read)
			continue;
		/* we've read the entire message, reset and continue */
		else if (bytes_total_read == bytes_to_read)
			bytes_total_read = 0;
		/* something is off, we've read more than we should
		 * this really should not happen */
		else
		{
			errorf("Pipe overrun: %d\n", bytes_total_read);
			fail_fast();
		}

		/* keep track of how many msgs we've received */
		msg_count++;

		/* size 9 is a checkpoint msg */
		if (msg->reflen == 9)
			kafka_checkpoint_callback(msg->err, msg->ref, msg->reflen);
		/* or else it's a legit copydata msg */
		else
			kafka_callback(msg->err, msg->ref, msg->reflen);

		/* the msg is no longer used so set it free */
		free(msg);
	}

	/* if this isn't an non-blocking error, something is wrong */
	if (bytes_read < 0 && errno != EAGAIN)
	{
		errorf("Pipe read failed: %s\n", strerror(errno));
		fail_fast();
	}

	/* we haven't read an entire msg, but there is nothing left on the pipe */
	if (bytes_total_read > 0)
	{
		errorf("Pipe has torn data: %d\n", bytes_total_read);
		fail_fast();
	}

	/* if we processed a msg(s), send a checkpoint */
	if (msg_count > 0)
		kafka_checkpoint();
}

static void
start_replication_streaming(void)
{
	/* for the replication options loop */
	int i;

	/* we need to send a few queries to the server to start replication */
	PQExpBuffer query = createPQExpBuffer();
	PGresult *res;

	/* first, grab a connection to the server */
	if (!conn)
		conn = GetConnection();

	/* error message already written in the connection */
	if (!conn)
		return;

	/* let's start up the streaming replication */
	debugf("Starting log streaming at %X/%X (slot %s)\n",
		   (uint32) (output_start_lsn >> 32), (uint32) output_start_lsn,
		   replication_slot);

	/* initiate the replication stream at specified location */
	appendPQExpBuffer(query, "START_REPLICATION SLOT \"%s\" LOGICAL %X/%X",
					  replication_slot,
					  (uint32) (output_start_lsn >> 32), (uint32) output_start_lsn);

	/* print replication slot options if there are any */
	if (noptions)
	{
		appendPQExpBufferStr(query, " (");

		/* build up the option query */
		for (i = 0; i < noptions; i++)
		{
			/* separator */
			if (i > 0)
				appendPQExpBufferStr(query, ", ");

			/* write option name */
			appendPQExpBuffer(query, "\"%s\"", options[(i * 2)]);

			/* write option value if specified */
			if (options[(i * 2) + 1] != NULL)
				appendPQExpBuffer(query, " '%s'", options[(i * 2) + 1]);
		}

		appendPQExpBufferChar(query, ')');
	}

	/* try to start streaming replication */
	res = PQexec(conn, query->data);

	/* make sure we're up and running */
	if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		errorf("Could not send replication command \"%s\": %s",
			   query->data, PQresultErrorMessage(res));
		fail_fast();
	}

	/* cleanup */
	PQclear(res);
	resetPQExpBuffer(query);

	/* set the last keepalive to now */
	last_keepalive_us = feGetCurrentTimestamp();
}

/*
 * We use async IO, and sometimes no data is available. In that case, we block
 * on reading, but not more than our keepalive interval.
 */
static void
wait_replication_stream(void)
{
	int ret;

	/* this is how select() tracks timeout */
	int64 next_keepalive_time_us = 0;

	/* file descriptors for indicating io read readiness */
	fd_set read_fds;

	/* this is how select() tracks timeout */
	struct timeval timeout;

	/* watch the pg connection and kafka deliv pipe for incoming io */
	FD_ZERO(&read_fds);
	FD_SET(PQsocket(conn), &read_fds);
	FD_SET(kafka_msg_delivery_pipe, &read_fds);

	/* compute when we need to wakeup to send a keepalive message
	 * offset by one ms to make sure we hit the keepalive target */
	next_keepalive_time_us = last_keepalive_us +
		keepalive_interval_us - ((int64) 1000);

	/* now compute how long to wait on io */
	long secs;
	int usecs;

	/* compute the difference between now and our next keepalive */
	feTimestampDifference(feGetCurrentTimestamp(),
						  next_keepalive_time_us,
						  &secs, &usecs);

	/* sleep at least 1 sec */
	timeout.tv_sec = secs <= 0 ? 1 : secs;
	timeout.tv_usec = usecs;

	/* block until we a) receive io or b) timeout */
	ret = select(Max(PQsocket(conn), kafka_msg_delivery_pipe) + 1,
				 &read_fds, NULL, NULL, &timeout);

	/* zero means success, -1 and EINTR mean the timeout hit */
	if (ret == 0 || (ret < 0 && errno == EINTR))
	{
		/* let the caller continue */
		return;
	}
	/* something is hosed */
	else if (ret < 0)
	{
		errorf("select() failed: %s\n", strerror(errno));
		fail_fast();
	}
	/* we actually have some data */
	else
	{
		/* this a behinds the scene call that reads data so another
		 * select won't block */
		if (PQconsumeInput(conn) == 0)
		{
			errorf("Could not receive data from WAL stream: %s",
				   PQerrorMessage(conn));
			fail_fast();
		}
	}
}

static void
consume_replication_stream(void)
{
	int ret;

	/* the copybuf is the current streamed log entry */
	char *copybuf = NULL;
	int copylen;

	/* read the header of the XLogData message, enclosed in the CopyData
	 * message.
	 * we only need the WAL location field (dataStart), so the rest of the
	 * header is ignored.
	 * 25 == msgtype 'w' + dataStart + walEnd + sendTime */
	int hdrlen = 25;

	/* we haven't written anything yet, so */
	output_written_lsn = InvalidXLogRecPtr;
	output_fsync_lsn = InvalidXLogRecPtr;

	/* keep spinning until we're told otherwise */
	while (!time_to_abort)
	{
		/* make sure we have a clean copy buffer */
		if (copybuf != NULL)
			copybuf = NULL;

		/* check if we've had any msg deliveries */
		kafka_read_msg_delivery_pipe();

		/* send a keepalive if we need to */
		maybe_send_keepalive();

		/* try to get data asynchronously */
		copylen = PQgetCopyData(conn, &copybuf, 1);

		/* we didn't get anything, so we need to wait */
		if (copylen == 0)
		{
			/* wait in a smart way */
			wait_replication_stream();

			/* and then start over */
			continue;
		}

		/* end of copy stream, so start exiting */
		if (copylen == -1)
		{
			errorf("Connection closed\n");
			fail_fast();
		}

		/* failure while reading the copy stream */
		if (copylen <= -2)
		{
			errorf("Could not read copy data: %s", PQerrorMessage(conn));
			fail_fast();
		}

		/* we have a keepalive message */
		if (copybuf[0] == 'k')
		{
			/* get it handled correctly */
			handle_keepalive(copybuf, copylen);

			/* free the mem */
			PQfreemem(copybuf);

			/* and then start over */
			continue;
		}
		/* this is a message we have no idea how to handle */
		else if (copybuf[0] != 'w')
		{
			errorf("Unrecognized streaming header: \"%c\"\n", copybuf[0]);
			fail_fast();
		}

		/* double-check everything is legit */
		if (copylen < hdrlen + 1)
		{
			errorf("Streaming header too small: %d\n", copylen);
			fail_fast();
		}

		/* send the message body to kafka.
		 * the kafka lib will make a callback with the full payload when the
		 * msg is sent or fails */
		ret = kafka_send_msg(copybuf + hdrlen, copylen - hdrlen,
							 copybuf + 1, 8, /* lsn */
							 copybuf, copylen);
		if (ret < 0)
		{
			errorf("Could not send to kafka: %d\n", ret);
			fail_fast();
		}

		/* extract wal location for this block and record that we sent */
		output_written_lsn = fe_recvint64(&copybuf[1]);
	}
}

static void
sigint_handler(int signum)
{
	/* ctrl-c'd */
	time_to_abort = true;
}

static void
parse_replication_options(char *optarg)
{
	char *data = pg_strdup(optarg);
	char *val = strchr(data, '=');

	if (val != NULL)
	{
		/* remove =; separate data from val */
		*val = '\0';
		val++;
	}

	noptions += 1;
	options = pg_realloc(options, sizeof(char *) * noptions * 2);

	options[(noptions - 1) * 2] = data;
	options[(noptions - 1) * 2 + 1] = val;
}

static void
parse_start_position(char* optarg)
{
	uint32 hi, lo;

	if (sscanf(optarg, "%X/%X", &hi, &lo) != 2)
	{
		errorf("Could not parse start position \"%s\"\n", optarg);
		exit(1);
	}

	output_start_lsn = ((uint64) hi) << 32 | lo;
}

int
main(int argc, char **argv)
{
	static struct option long_options[] = {
		{"file", required_argument, NULL, 'f'},
		{"verbose", no_argument, NULL, 'v'},
		{"dbname", required_argument, NULL, 'd'},
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"username", required_argument, NULL, 'U'},
		{"no-password", no_argument, NULL, 'w'},
		{"password", no_argument, NULL, 'W'},
		{"topic", required_argument, NULL, 't'},
		{"brokers", required_argument, NULL, 'b'},
		{"output_start_lsn", required_argument, NULL, 'I'},
		{"option", required_argument, NULL, 'o'},
		{"status-interval", required_argument, NULL, 's'},
		{"slot", required_argument, NULL, 'S'},
		{NULL, 0, NULL, 0}
	};

	int c;
	int option_index;

	while ((c = getopt_long(argc, argv, "f:F:nvd:h:p:U:wWI:o:s:S:t:b:",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'v':
				verbose++;
				break;
			case 'd':
				dbname = pg_strdup(optarg);
				break;
			case 'h':
				dbhost = pg_strdup(optarg);
				break;
			case 'p':
				dbport = pg_strdup(optarg);
				break;
			case 'U':
				dbuser = pg_strdup(optarg);
				break;
			case 'w':
				dbgetpassword = -1;
				break;
			case 'W':
				dbgetpassword = 1;
				break;
			case 'I':
				parse_start_position(optarg);
				break;
			case 'o':
				parse_replication_options(optarg);
				break;
			case 's':
				keepalive_interval_us = atoi(optarg) * ((int64) 1000000);
				break;
			case 'S':
				replication_slot = pg_strdup(optarg);
				break;
			case 't':
				kafka_topic = pg_strdup(optarg);
				break;
			case 'b':
				kafka_brokers = pg_strdup(optarg);
				break;
			default:
				errorf("TODO welp\n");
				exit(1);
		}
	}

	/* handle ctrl-c's properly */
	pqsignal(SIGINT, sigint_handler);

	/* try to get kafka up and running */
	if (!kafka_init())
	{
		errorf("Could not initiate kafka\n");
		fail_fast();
	}

	/* start up the replication stream from pg */
	start_replication_streaming();

	/* consume until we're killed or error */
	consume_replication_stream();
}
