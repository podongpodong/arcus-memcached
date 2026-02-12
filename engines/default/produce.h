#ifndef PRODUCE_H
#define PRODUCE_H

/* kafka init */
ENGINE_ERROR_CODE kafka_st_init(char *topic, char *broker);

/* ------ produce function prototype ------*/
ENGINE_ERROR_CODE ms_prod_init(struct default_engine* engine);
ENGINE_ERROR_CODE producer_thread_start(void);
void producer_thread_stop(void);
void producer_final(void);

/* util function */
void set_cmdlog_file(char *data_path, int64_t lasttime);
void do_snapshot_to_cmdlog(void);
void do_cmdlog_to_snapshot(void);

#endif
