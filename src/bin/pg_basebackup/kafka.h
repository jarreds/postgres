#include <librdkafka/rdkafka.h>
#include <sys/queue.h>

#define debugf(format, ...) if (!verbose); else fprintf(stderr, format, ##__VA_ARGS__)
#define errorf(format, ...) fprintf(stderr, format, ##__VA_ARGS__)

enum kafka_msg_status { PENDING, DELIVERED, FAIL };

typedef struct kafka_msg_struct kafka_msg;
struct kafka_msg_struct {
	enum kafka_msg_status status;
	int err;
	void *ref;
	size_t reflen;
	STAILQ_ENTRY(kafka_msg_struct) entries;
};

extern char *kafka_brokers;
extern char *kafka_topic;

extern int kafka_msg_delivery_pipe;

extern int kafka_init(void);
extern void kafka_destroy(void);
extern int kafka_send_msg(void *payload, size_t paylen,
						  void *key, size_t keylen,
						  void *msg_opaque, size_t msg_opaquelen);
