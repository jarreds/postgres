#include <stdlib.h>
#include <sys/queue.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <string.h>

#include "kafka.h"

/* let whoever set these */
char *kafka_brokers;
char *kafka_topic;
int kafka_msg_delivery_pipe;

/* right now, we send everything to the same partition */
static int partition = 0;

/* librdkafka internal config and whatnot */
static rd_kafka_t *rk;
static rd_kafka_conf_t *conf = NULL;
static rd_kafka_topic_t *rkt = NULL;
static rd_kafka_topic_conf_t *topic_conf = NULL;

/* use these to communicate with the caller */
static pthread_t msg_cb_poller = NULL;
static int msg_cb_pipes[2];

/* to break our separate thread polling loop */
static volatile sig_atomic_t time_to_abort = 0;

/* librdkafka sends messages async with callbacks -- in order to guarantee
 * callback ordering we place all requests in a fifo queue and callback in the
 * correct order */
STAILQ_HEAD(fifo, kafka_msg_struct) head = STAILQ_HEAD_INITIALIZER(head);

/* enforce that our delivery callbacks are made in the same order the msgs
 * were produced -- we need this to replicate the wal log correctly */
static void
kafka_send_msg_fifo_cb(void)
{
	/* the head of the fifo queue */
	kafka_msg *first = NULL;

	/* number of bytes we're going to write to the pipe */
	int bytes_to_write = sizeof(first);

	/* how many bytes we've written per write */
	int bytes_written = 0;

	/* how many bytes we've written per msg */
	int bytes_total_written = 0;

	/* iterate the fifo queue for non-pending msgs and callback on those */
	while ((first = STAILQ_FIRST(&head)) && first->status != PENDING)
	{
		/* remove the msg from the queue */
		STAILQ_REMOVE_HEAD(&head, entries);

		/* loop until we've written everything to the pipe */
		while ((bytes_written = write(msg_cb_pipes[1],
									  &first + bytes_total_written,
									  bytes_to_write - bytes_total_written)) >= 0)
		{
			/* keep track of how much we've written in total for this msg */
			bytes_total_written += bytes_written;

			/* ensure we've written the entire msg, or write more */
			if (bytes_total_written < bytes_to_write)
				continue;
			/* we've written the entire msg, reset and break */
			else if (bytes_total_written == bytes_to_write)
			{
				bytes_total_written = 0;
				break;
			}
		}

		/* something broke */
		if (bytes_written < 0)
		{
			errorf("Pipe write failed: %s\n", strerror(errno));
			kill(getpid(), SIGINT);
		}
		/* we didn't write everything */
		else if (bytes_total_written > 0)
		{
			errorf("Pipe write incomplete: %d\n", bytes_total_written);
			kill(getpid(), SIGINT);
		}
	}
}

void
kafka_destroy(void)
{
	/* kill the poll loop */
	time_to_abort = 1;

	if (rkt)
		rd_kafka_topic_destroy(rkt);

	if (rk)
		rd_kafka_destroy(rk);

	/* make sure the poll loop is dead */
	pthread_join(msg_cb_poller, NULL);

	/* cleanup the pipe */
	close(msg_cb_pipes[0]);
	close(msg_cb_pipes[1]);
}

static void*
kafka_poll_loop(void *arg)
{
	/* do what we do to check for deliveries */
	while (!time_to_abort)
		if (rd_kafka_poll(rk, 1000) > 0)
			kafka_send_msg_fifo_cb();

	return NULL;
}

int
kafka_send_msg(void *payload, size_t paylen,
			   void *key, size_t keylen,
			   void *msg_opaque, size_t msg_opaquelen)
{
	int res;

	/* if we fill the librbkafka send buffer we apply 1ms backpressure */
	struct timeval backpressure;
	backpressure.tv_sec = 0;
	backpressure.tv_usec = 1000;

	/* wrap the msg in a tracking structure for our fifo queue */
	kafka_msg *msg = malloc(sizeof(kafka_msg));
	msg->status = PENDING; /* it's pending */
	msg->err = 0; /* no error yet */
	msg->ref = msg_opaque; /* extra data used for caller tracking */
	msg->reflen = msg_opaquelen; /* extra data length */

	/* send the msg to librdkafka for async delivery */
	while ((res = rd_kafka_produce(rkt, partition, 0,
								   payload, paylen,
								   key, keylen,
								   msg)) == -1 &&
		   errno == ENOBUFS)
	{
		/* don't spin if we're aborting*/
		if (time_to_abort) return res;

		fprintf(stderr, "Applying kafka send backpressure\n");

		/* apply backpressure for 1 ms when we've queued too many msgs */
		select(0, NULL, NULL, NULL, &backpressure);
	}

	if (res == -1)
	{
		/* welp */
		free(msg);
		fprintf(stderr, "Failed to produce to topic %s partition %i: %s\n",
				rd_kafka_topic_name(rkt), partition,
				rd_kafka_err2str(rd_kafka_errno2err(res)));
	}
	else
	{
		/* librbkafka has the msg, put it in our fifo queue */
		STAILQ_INSERT_TAIL(&head, msg, entries);
	}

	return res;
}

/* librdkafka calls this when it has delivered or failed a msg
 * kafka_poll to make sure this is called */
static void
kafka_send_msg_cb(rd_kafka_t *rk,
				  void *payload, size_t paylen,
				  rd_kafka_resp_err_t err,
				  void *opaque, void *msg_opaque)
{
	/* extra data that the caller is using for tracking */
	kafka_msg *msg = (kafka_msg*)msg_opaque;

	if (err)
	{
		/* welp, mark the message as fail */
		msg->status = FAIL;
		errorf("Failed to produce to topic %s partition %i: %s\n",
			   rd_kafka_topic_name(rkt), partition,
			   rd_kafka_err2str(rd_kafka_errno2err(err)));
	}
	else
	{
		/* it's all good */
		msg->status = DELIVERED;
	}

	/* maybe an err, maybe not -- just pass it through */
	msg->err = err;
}

static void
kafka_error_cb(rd_kafka_t *rk,
			   int err, const char *reason,
			   void *opaque)
{
	errorf("Kafka error: %s: %s\n", rd_kafka_err2str(err), reason);
	kill(getpid(), SIGINT);
}

int
kafka_init(void)
{
	int res;
	char errstr[512];

	STAILQ_INIT(&head);

	conf = rd_kafka_conf_new();
	topic_conf = rd_kafka_topic_conf_new();

	rd_kafka_conf_set_error_cb(conf, kafka_error_cb);
	rd_kafka_conf_set_dr_cb(conf, kafka_send_msg_cb);

	res = rd_kafka_conf_set(conf, "compression.codec", "snappy",
							errstr, sizeof(errstr));
	if (res != RD_KAFKA_CONF_OK)
	{
		errorf("%s\n", errstr);
		exit(1);
	}

	res = rd_kafka_conf_set(conf, "queue.buffering.max.ms", "25",
							errstr, sizeof(errstr));
	if (res != RD_KAFKA_CONF_OK)
	{
		errorf("%s\n", errstr);
		exit(1);
	}

	res = rd_kafka_conf_set(conf, "queue.buffering.max.messages", "100000",
							errstr, sizeof(errstr));
	if (res != RD_KAFKA_CONF_OK)
	{
		errorf("%s\n", errstr);
		exit(1);
	}

	rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
					  errstr, sizeof(errstr));
	if (!rk)
	{
		errorf("Producer failed: %s\n", errstr);
		exit(1);
	}

	res = rd_kafka_brokers_add(rk, kafka_brokers);
	if (res == 0)
	{
		errorf("No valid brokers given\n");
		exit(1);
	}

	rkt = rd_kafka_topic_new(rk, kafka_topic, topic_conf);
	if (!rkt)
	{
		errorf("Topic failed: %s\n", errstr);
		exit(1);
	}

	res = pipe(msg_cb_pipes);
	if (res == -1)
	{
		errorf("Could not create msg polling pipe: %d\n", res);
		exit(1);
	}

	kafka_msg_delivery_pipe = msg_cb_pipes[0];
	fcntl(kafka_msg_delivery_pipe, F_SETFL, O_NONBLOCK);

	res = pthread_create(&msg_cb_poller, NULL, kafka_poll_loop, NULL);
	if (res)
	{
		errorf("Could not create msg polling thread: %d\n", res);
		exit(1);
	}

	return 1;
}
