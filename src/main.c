/*
 * dist-bcast-sim: Milestone 1 (BEB)
 *
 * Discrete-event simulation (single OS process) of:
 *  - N processes
 *  - best-effort broadcast over an unreliable async network
 *
 * Next milestones:
 *  - RB: eager rebroadcast + duplicate suppression
 *  - URB: majority/threshold delivery
 *  - FIFO on URB: per-sender seq buffering
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#define MAX_PROCS 16
#define MAX_EVENTS 100000
#define MAX_PAYLOAD 64

/* -------------------- Config -------------------- */

typedef struct {
    int nprocs;          /* number of processes */
    int max_time;        /* stop after this simulated time */
    int max_delay;       /* network delay in ticks: [0..max_delay] */
    int drop_percent;    /* 0..100 probability message is dropped */
    unsigned seed;       /* RNG seed */
} SimConfig;

/* -------------------- Message/Event Types -------------------- */

typedef struct {
    int src;            /* immediate sender (who sent this packet) */
    int dst;            /* receiver */
    int origin;         /* original broadcaster (for later abstractions) */
    int seq;            /* per-origin sequence number (used later) */
    char payload[MAX_PAYLOAD];
} NetMsg;

typedef struct {
    int time;           /* simulated delivery time */
    NetMsg msg;
    int active;         /* 1 if this slot is used */
} Event;

/* -------------------- Process State -------------------- */

typedef struct {
    int pid;
    int crashed;        /* for later: simulate process crashes */
    /* delivered logs / seen sets go here in later milestones */
} Proc;

/* -------------------- Globals (simple, for now) -------------------- */

static SimConfig g_cfg;
static Proc g_procs[MAX_PROCS];
static Event g_events[MAX_EVENTS];
static int g_now = 0;

/* -------------------- Utilities -------------------- */

static void die(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

static int randint(int lo, int hi) {
    /* inclusive range */
    if (hi < lo) return lo;
    return lo + (rand() % (hi - lo + 1));
}

static int chance_drop(int drop_percent) {
    /* returns 1 if should drop */
    if (drop_percent <= 0) return 0;
    if (drop_percent >= 100) return 1;
    return (randint(1, 100) <= drop_percent);
}

/* -------------------- Event Queue (simple scan) -------------------- */
/*
 * We store events in an array and pick the next event by scanning for
 * minimum time. This is O(#events) per step but very easy to read.
 * Later you can replace with a binary heap.
 */

static int event_add(int deliver_time, const NetMsg *m) {
    for (int i = 0; i < MAX_EVENTS; i++) {
        if (!g_events[i].active) {
            g_events[i].active = 1;
            g_events[i].time = deliver_time;
            g_events[i].msg = *m;
            return 0;
        }
    }
    return -1; /* full */
}

static int event_pop_next(Event *out) {
    int best_idx = -1;
    int best_time = 0;

    for (int i = 0; i < MAX_EVENTS; i++) {
        if (!g_events[i].active) continue;
        if (best_idx < 0 || g_events[i].time < best_time) {
            best_idx = i;
            best_time = g_events[i].time;
        }
    }

    if (best_idx < 0) return 0; /* none */

    *out = g_events[best_idx];
    g_events[best_idx].active = 0;
    return 1;
}

/* -------------------- Network + BEB -------------------- */

static void net_send(int src, int dst, const char *payload, int origin, int seq) {
    if (chance_drop(g_cfg.drop_percent)) {
        /* dropped in the network */
        return;
    }

    NetMsg m;
    m.src = src;
    m.dst = dst;
    m.origin = origin;
    m.seq = seq;
    strncpy(m.payload, payload, MAX_PAYLOAD - 1);
    m.payload[MAX_PAYLOAD - 1] = '\0';

    int delay = randint(0, g_cfg.max_delay);
    int deliver_time = g_now + delay;

    if (event_add(deliver_time, &m) != 0) {
        fprintf(stderr, "event queue full\n");
        exit(EXIT_FAILURE);
    }
}

/* Best Effort Broadcast: sender sends to all (including self) */
static void beb_broadcast(int sender, const char *payload) {
    /* For later abstractions, origin/seq become real. For BEB, keep simple. */
    int origin = sender;
    static int next_seq[MAX_PROCS]; /* per-origin */
    int seq = next_seq[origin]++;

    for (int dst = 0; dst < g_cfg.nprocs; dst++) {
        net_send(sender, dst, payload, origin, seq);
    }
}

/* What happens when a process receives a packet from the network */
static void beb_deliver(int dst, const NetMsg *m) {
    /* For BEB, "deliver" is just "received from network" */
    printf("t=%d BEB_DELIVER dst=%d  (src=%d origin=%d seq=%d) payload=\"%s\"\n",
           g_now, dst, m->src, m->origin, m->seq, m->payload);
}

/* -------------------- Simulation -------------------- */

static void sim_init(const SimConfig *cfg) {
    g_cfg = *cfg;
    g_now = 0;
    memset(g_events, 0, sizeof(g_events));
    memset(g_procs, 0, sizeof(g_procs));

    if (g_cfg.nprocs <= 0 || g_cfg.nprocs > MAX_PROCS) {
        fprintf(stderr, "nprocs must be 1..%d\n", MAX_PROCS);
        exit(EXIT_FAILURE);
    }

    srand(g_cfg.seed);

    for (int i = 0; i < g_cfg.nprocs; i++) {
        g_procs[i].pid = i;
        g_procs[i].crashed = 0;
    }
}

static void sim_run(void) {
    Event ev;

    while (g_now <= g_cfg.max_time) {
        if (!event_pop_next(&ev)) {
            /* no more events */
            break;
        }

        /* advance time */
        g_now = ev.time;

        int dst = ev.msg.dst;
        if (dst < 0 || dst >= g_cfg.nprocs) continue;

        if (g_procs[dst].crashed) {
            /* later: decide what "crashed" means */
            continue;
        }

        beb_deliver(dst, &ev.msg);
    }
}

/* -------------------- Main -------------------- */

int main(int argc, char **argv) {
    (void)argc; (void)argv;

    SimConfig cfg;
    cfg.nprocs = 5;
    cfg.max_time = 200;
    cfg.max_delay = 10;
    cfg.drop_percent = 20; /* try 0, 20, 60 */
    cfg.seed = (unsigned)time(NULL);

    sim_init(&cfg);

    /* Demo traffic: a couple broadcasts at time 0 */
    beb_broadcast(0, "hello from p0");
    beb_broadcast(3, "hello from p3");
    beb_broadcast(0, "second from p0");

    sim_run();
    return 0;
}

