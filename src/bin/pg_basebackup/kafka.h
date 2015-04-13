#include <librdkafka/rdkafka.h>

#define debugf(format, ...) if (!verbose); else fprintf(stderr, format, ##__VA_ARGS__)
#define errorf(format, ...) fprintf(stderr, format, ##__VA_ARGS__)

extern char *kafka_brokers;
extern char *kafka_topic;

extern int kafka_init(void);
extern void kafka_destroy(void);
extern int kafka_poll(void);
extern int kafka_send_msg(void *payload, size_t paylen,
						  void *key, size_t keylen,
						  void *msg_opaque, size_t msg_opaquelen,
						  void (*cb) (int err, void *opaque, size_t opaquelen));
