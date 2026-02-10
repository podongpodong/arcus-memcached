#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <dirent.h>
#include <unistd.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <signal.h>
#include <sys/time.h>

#include "rdkafka.h"

#define ENABLE_PERSISTENCE 1
#include <memcached/engine.h>
#include "engines/default/default_engine.h"
#include "engines/default/item_base.h"
#include "engines/default/cmdlogrec.h"

#define DST "/home/intern/minuk/kafka_test/copy_snapshot"

typedef struct _kafka_st {
    rd_kafka_t *rk;
    char *topic;
    char *brokers;
} kafka_st;

typedef struct snapshot_ctx {
    char keybuf[1024];
    uint16_t keylen;
    uint8_t ittype;
} snapshot_ctx;

/* global data */
static kafka_st kafka_anch;

void kafka_st_init(char *topic, char *broker)
{
    kafka_anch.topic = "test-topic";
    kafka_anch.brokers = "localhost";
}

void consumer_init(int argc, char **argv) {
    rd_kafka_t *rk;
    rd_kafka_conf_t *conf;
    rd_kafka_topic_partition_list_t *subscription;
    char errstr[512];

    conf = rd_kafka_conf_new();

    if(rd_kafka_conf_set(conf, "bootstrap.servers", kafka_anch.brokers, errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return;
    }

    if(rd_kafka_conf_set(conf, "group.id", argv[2], errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return;
    }

    if(rd_kafka_conf_set(conf, "group.protocol", argv[3], errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return;
    }

    if(rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return;
    }

    rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) {
        fprintf(stderr, "%% Failed to create new consumer: %s\n", errstr);
        exit(1);
    }
    conf = NULL;
    kafka_anch.rk = rk;

    rd_kafka_poll_set_consumer(rk);
    subscription = rd_kafka_topic_partition_list_new(argc-4);
    for (int i = 4; i<argc; i++)
        rd_kafka_topic_partition_list_add(subscription, argv[i], RD_KAFKA_PARTITION_UA);

    rd_kafka_resp_err_t err;
    err = rd_kafka_subscribe(rk, subscription);
    if (err) {

                fprintf(stderr, "%% Failed to subscribe to %d topics: %s\n",
                        subscription->cnt, rd_kafka_err2str(err));
                rd_kafka_topic_partition_list_destroy(subscription);
                rd_kafka_destroy(rk);
    }
}

static bool lrec_append_raw(char **bufptr, size_t *remaining, const void *data, size_t len)
{
    if (len > *remaining) {
        return false;
    }
    memcpy(*bufptr, data, len);
    *bufptr += len;
    *remaining -= len;
    return true;
}

static bool lrec_append_fmt(char **bufptr, size_t *remaining, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int written = vsnprintf(*bufptr, *remaining, fmt, ap);
    va_end(ap);
    if (written < 0 || (size_t)written >= *remaining) {
        return false;
    }
    *bufptr += written;
    *remaining -= (size_t)written;
    return true;
}

static bool append_raw(char **bufptr, size_t *remaining, const void *data, size_t len)
{
    if (len > *remaining) {
        return false;
    }
    memcpy(*bufptr, data, len);
    *bufptr += len;
    *remaining -= len;
    return true;
}

static bool append_fmt(char **bufptr, size_t *remaining, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int written = vsnprintf(*bufptr, *remaining, fmt, ap);
    va_end(ap);
    if (written < 0 || (size_t)written >= *remaining) {
        return false;
    }
    *bufptr += written;
    *remaining -= (size_t)written;
    return true;
}

static bool append_bkey(char **bufptr, size_t *remaining, uint8_t nbkey, const unsigned char *bkey)
{
    if (nbkey == 0) {
        return append_fmt(bufptr, remaining, "%" PRIu64, *(const uint64_t *)bkey);
    }

    char bkey_hex[MAX_BKEY_LENG * 2 + 2];
    safe_hexatostr(bkey, nbkey, bkey_hex);
    return append_fmt(bufptr, remaining, "0x%s", bkey_hex);
}

static bool append_eflag(char **bufptr, size_t *remaining, uint8_t neflag, const unsigned char *eflag)
{
    char eflag_hex[MAX_EFLAG_LENG * 2 + 2];
    safe_hexatostr(eflag, neflag, eflag_hex);
    return append_fmt(bufptr, remaining, "0x%s", eflag_hex);
}

static bool snapshot_elem_to_ascii(const SnapshotElemLog *log, const snapshot_ctx *ctx,
                                   char *buf, size_t bufsize, size_t *cmdlen)
{
    char *bufptr = buf;
    size_t remaining = bufsize;
    const SnapshotElemData *body = &log->body;
    const char *keyptr = ctx->keybuf;
    uint16_t keylen = ctx->keylen;
    bool ok = false;

    if (keylen == 0) {
        return false;
    }

    if (ctx->ittype == ITEM_TYPE_LIST) {
        uint32_t bytes = (body->nbytes >= 2 ? body->nbytes - 2 : body->nbytes);
        ok = append_fmt(&bufptr, &remaining, "lop insert %.*s -1 %u\r\n",
                        keylen, keyptr, bytes) &&
             append_raw(&bufptr, &remaining, body->data, body->nbytes);
    } else if (ctx->ittype == ITEM_TYPE_SET) {
        uint32_t bytes = (body->nbytes >= 2 ? body->nbytes - 2 : body->nbytes);
        ok = append_fmt(&bufptr, &remaining, "sop insert %.*s %u\r\n",
                        keylen, keyptr, bytes) &&
             append_raw(&bufptr, &remaining, body->data, body->nbytes);
    } else if (ctx->ittype == ITEM_TYPE_MAP) {
        uint32_t bytes = (body->nbytes >= 2 ? body->nbytes - 2 : body->nbytes);
        const char *fieldptr = body->data;
        const char *valptr = fieldptr + body->nekey;
        ok = append_fmt(&bufptr, &remaining, "mop insert %.*s %.*s %u\r\n",
                        keylen, keyptr, body->nekey, fieldptr, bytes) &&
             append_raw(&bufptr, &remaining, valptr, body->nbytes);
    } else if (ctx->ittype == ITEM_TYPE_BTREE) {
        uint32_t bytes = (body->nbytes >= 2 ? body->nbytes - 2 : body->nbytes);
        const unsigned char *bkeyptr = (const unsigned char *)body->data;
        const unsigned char *eflagptr = bkeyptr + (body->nekey==0 ? sizeof(uint64_t) : (body->nekey));
        const char *valptr = (const char *)(eflagptr + body->neflag);

        ok = append_fmt(&bufptr, &remaining, "bop insert %.*s ",
                        keylen, keyptr) &&
             append_bkey(&bufptr, &remaining, body->nekey, bkeyptr);
        if (ok && body->neflag > 0) {
            ok = append_fmt(&bufptr, &remaining, " ") &&
                 append_eflag(&bufptr, &remaining, body->neflag, eflagptr);
        }
        if (ok) {
            ok = append_fmt(&bufptr, &remaining, " %u\r\n", bytes) &&
                 append_raw(&bufptr, &remaining, valptr, body->nbytes);
        }
    }

    *cmdlen = (size_t)(bufptr - buf);
    return ok;
}

static void update_snapshot_ctx(snapshot_ctx *ctx, LogRec *logrec)
{
    ITLinkLog  *log  = (ITLinkLog*)logrec;
    ITLinkData *body = &log->body;
    struct lrec_item_common cm = body->cm;
    char *keyptr = body->data;

    if (cm.ittype == ITEM_TYPE_BTREE) {
        const struct lrec_coll_meta *meta = (const struct lrec_coll_meta *)&body->ptr.meta;
        if (meta->maxbkrlen != BKEY_NULL) {
            keyptr += (meta->maxbkrlen==0 ? sizeof(uint64_t) : (meta->maxbkrlen));
        }
    }

    if (cm.ittype != ITEM_TYPE_KV) {
        // ctx->keylen = cm->keylen;
        // ctx->ittype = cm->ittype;
        // if (cm->keylen > 0 && cm->keylen < sizeof(ctx->keybuf)) {
        //     memcpy(ctx->keybuf, keyptr, cm->keylen);
        //     ctx->keybuf[cm->keylen] = '\0';
        // }
    }
}

bool lrec_to_ascii_command(LogRec *logrec, char *buf, size_t bufsize, size_t *cmdlen)
{
    char *bufptr = buf;
    size_t remaining = bufsize;
    bool ok = false;

    if (buf == NULL || bufsize == 0 || cmdlen == NULL) {
        return false;
    }

    switch (logrec->header.logtype) {
        case LOG_IT_LINK: {
            ITLinkLog  *log  = (ITLinkLog*)logrec;
            ITLinkData *body = &log->body;
            struct lrec_item_common cm = body->cm;
            char *keyptr = body->data;

            if (cm.ittype == ITEM_TYPE_KV) {
                char *valptr = keyptr + cm.keylen;
                uint32_t bytes = (cm.vallen >= 2 ? cm.vallen - 2 : cm.vallen);
                ok = lrec_append_fmt(&bufptr, &remaining, "set %.*s %u %u %u\r\n",
                                     cm.keylen, keyptr, cm.flags, cm.exptime, bytes) &&
                     lrec_append_raw(&bufptr, &remaining, valptr, cm.vallen);
            }
        } break;
        case LOG_OPERATION_BEGIN:
        case LOG_OPERATION_END:
        case LOG_SNAPSHOT_DONE:
            ok = false;
            break;
    }
    *cmdlen = (size_t)(bufptr - buf);
    return ok;
}

static int do_consum(int fd) {
    kafka_st *ks = &kafka_anch;

    snapshot_ctx ctx;
    memset(&ctx, 0, sizeof(snapshot_ctx));

	while(1) {
		rd_kafka_message_t *rkm;

		// 메세지 큐에서 메세지를 하나 소비
		rkm = rd_kafka_consumer_poll(ks->rk, 500);

		if (rkm == NULL) {
			continue; // timeout
		}

		if (rkm->err) {
			rd_kafka_message_destroy(rkm);
			continue;
		}

		char buf[4096];
        memcpy(buf, rkm->payload, rkm->len);
        LogRec *logrec = (LogRec*)buf;
        LogHdr *loghdr = &logrec->header;

        if (rkm->len < sizeof(LogHdr) + loghdr->body_length) {
		    rd_kafka_message_destroy(rkm);
            continue;
        }

        char cmd_buf[4096];
        size_t cmd_len = 0;
        bool ok = false;

        if (loghdr->logtype == LOG_IT_LINK) {
            update_snapshot_ctx(&ctx, logrec);
            ok = lrec_to_ascii_command(logrec, cmd_buf, sizeof(cmd_buf), &cmd_len);
        } else if (loghdr->logtype == LOG_SNAPSHOT_ELEM) {
            ok = snapshot_elem_to_ascii((const SnapshotElemLog *)logrec, &ctx, cmd_buf, sizeof(cmd_buf), &cmd_len);
        } else {
            ok = lrec_to_ascii_command(logrec, cmd_buf, sizeof(cmd_buf), &cmd_len);
        }

		if (1) {
			rd_kafka_resp_err_t err = rd_kafka_commit_message(ks->rk, rkm, 0);
			if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
				// 커밋 실패 정책 필요
			}
		}

        if (ok && cmd_len > 0) {
            /* 전송 */
            cmd_buf[cmd_len] = '\0';
            printf("%s", cmd_buf);
        }

		rd_kafka_message_destroy(rkm);
	}
}

int main(int argc, char **argv)
{
kafka_st_init(NULL, NULL);
consumer_init(argc, argv);

    int fd;
    if (argc < 4) {
                fprintf(stderr,
                        "%% Usage: "
                        "%s <broker> <group.id> <group.protocol> <topic1> "
                        "<topic2>..\n",
                        argv[0]);
                return 1;
    }

    if ((fd=open(DST, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP)) < 0) {
        printf("Failed to open. PATH=%s\n", DST);
        return 0;
    }

    do_consum(fd);
}