#include <stdlib.h>
#include <sys/queue.h>
#include <unistd.h>

#include "kafka.h"

/* let whoever set these */
char *kafka_brokers;
char *kafka_topic;

/* right now, we send everything to the same partition */
static int partition = 0;

/* librdkafka internal config and whatnot */
static rd_kafka_t *rk;
static rd_kafka_conf_t *conf = NULL;
static rd_kafka_topic_t *rkt = NULL;
static rd_kafka_topic_conf_t *topic_conf = NULL;

/* a message can be in one of these states */
enum status { PENDING, DELIVERED, FAIL };

/* librdkafka sends messages async with callbacks -- in order to guarantee
 * callback ordering we place all requests in a fifo queue and callback in the
 * correct order */
typedef struct message_struct message;
struct message_struct {
	enum status stat;
	int err;
	void *ref;
	void (*cb) (int err, void *ref);
	STAILQ_ENTRY(message_struct) entries;
};

STAILQ_HEAD(fifo, message_struct) head = STAILQ_HEAD_INITIALIZER(head);

/* enforce that our delivery callbacks are made in the same order the msgs
 * were produced -- we need this to replicate the wal log correctly */
static void
kafka_send_msg_fifo_cb(void)
{
	/* the head of the fifo queue */
	message *first = NULL;

	/* iterate the fifo queue for non-pending msgs and callback on those */
	while ((first = STAILQ_FIRST(&head)) && first->stat != PENDING)
	{
		/* remove the msg from the queue */
		STAILQ_REMOVE_HEAD(&head, entries);

		/* make the callback to the calling code */
		first->cb(first->err, first->ref);

		/* release the internal fifo queue tracking entry */
		free(first);
	}
}

void
kafka_destroy(void)
{
	if (rkt)
		rd_kafka_topic_destroy(rkt);

	if (rk)
		rd_kafka_destroy(rk);
}

int
kafka_poll(void)
{
	/* we call this initiate delivery callbacks */
	return rd_kafka_poll(rk, 0);
}

int
kafka_send_msg(void *payload, size_t paylen,
			   void *key, size_t keylen,
			   void *msg_opaque,
			   void (*cb) (int err, void *opaque))
{
	int res;

	/* wrap the msg in a tracking structure for our fifo queue */
	message *msg = malloc(sizeof(message));
	msg->stat = PENDING; /* it's pending */
	msg->err = 0; /* no error yet */
	msg->ref = msg_opaque; /* extra data used for caller tracking */
	msg->cb = cb; /* callback that we call on delivery or fail */

	/* send the msg to librdkafka for async delivery */
	res = rd_kafka_produce(rkt, partition, 0,
						   payload, paylen,
						   key, keylen,
						   msg);

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
	message *msg = (message*)msg_opaque;

	if (err)
	{
		/* welp, mark the message as fail */
		msg->stat = FAIL;
		errorf("Failed to produce to topic %s partition %i: %s\n",
			   rd_kafka_topic_name(rkt), partition,
			   rd_kafka_err2str(rd_kafka_errno2err(err)));
	}
	else
	{
		/* it's all good */
		msg->stat = DELIVERED;
	}

	/* maybe an err, maybe not -- just pass it through */
	msg->err = err;

	/* initiate an fifo ordered callback to the calling code */
	kafka_send_msg_fifo_cb();
}

int
kafka_init(void)
{
	int res;
	char errstr[512];

	STAILQ_INIT(&head);

	conf = rd_kafka_conf_new();
	topic_conf = rd_kafka_topic_conf_new();

	rd_kafka_conf_set_dr_cb(conf, kafka_send_msg_cb);

	res = rd_kafka_conf_set(conf, "compression.codec", "snappy",
							errstr, sizeof(errstr));
	if (res != RD_KAFKA_CONF_OK)
	{
		errorf("%s\n", errstr);
		exit(1);
	}

	/* rd_kafka_set_logger(rk, rk_logger); */
	/* rd_kafka_set_log_level(rk, LOG_INFO); */

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

	return res;
}
