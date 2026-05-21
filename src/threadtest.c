#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <time.h>
#include <unistd.h>

#if defined(__QNXNTO__)
#include <sys/neutrino.h>
#endif

#define THREADTEST_SHM_NAME "/threadtest_event_hub"
#define THREADTEST_MAGIC 0x54545448u
#define THREADTEST_VERSION 1u
#define THREADTEST_MAX_EVENTS 64u
#define THREADTEST_EVENT_NAME_MAX 64u
#define THREADTEST_MAX_WAIT_EVENTS 16u
#define THREADTEST_OS_NAME_MAX 128u
#define THREADTEST_RELEASE_TEXT_MAX 90u

typedef enum {
    OS_KIND_LINUX,
    OS_KIND_QNX_71,
    OS_KIND_QNX_8,
    OS_KIND_QNX_OTHER,
    OS_KIND_UNKNOWN
} os_kind_t;

typedef struct {
    char name[THREADTEST_EVENT_NAME_MAX];
    uint64_t generation;
    uint64_t trigger_count;
    uint8_t in_use;
} shared_event_t;

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t cond_clock_id;
    uint32_t event_count;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    shared_event_t events[THREADTEST_MAX_EVENTS];
} shared_hub_t;

typedef struct {
    char instance_name[64];
    char event_names[THREADTEST_MAX_WAIT_EVENTS][THREADTEST_EVENT_NAME_MAX];
    size_t event_count;
    char set_event[THREADTEST_EVENT_NAME_MAX];
    char trigger_start_event[THREADTEST_EVENT_NAME_MAX];
    bool has_set_event;
    bool has_trigger_start_event;
    bool verbose;
    bool pin_to_cpu;
    unsigned int cpu_index;
    int sched_policy;
    int sched_priority;
    uint64_t timeout_ns;
    uint64_t work_ns;
    uint64_t runtime_ns;
} config_t;

typedef struct {
    uint64_t loop_iterations;
    uint64_t wait_calls;
    uint64_t triggers_received;
    uint64_t triggers_sent;
    uint64_t timeouts;
    uint64_t work_calls;
    uint64_t work_ns_total;
} stats_t;

typedef struct {
    shared_hub_t *hub;
    config_t config;
    int wait_event_indices[THREADTEST_MAX_WAIT_EVENTS];
    uint64_t seen_generations[THREADTEST_MAX_WAIT_EVENTS];
    int set_event_index;
    int trigger_start_index;
    stats_t stats;
    os_kind_t os_kind;
    const char *os_name;
    int result_code;
} app_t;

static volatile sig_atomic_t g_signal_stop = 0;
static volatile uint64_t g_work_sink = 0;

static void handle_signal(int signo)
{
    (void)signo;
    g_signal_stop = 1;
}

static void print_usage(FILE *stream, const char *program)
{
    fprintf(stream,
            "Usage: %s [options]\n"
            "\n"
            "Options:\n"
            "  -h, --help                 Show this help text\n"
            "  -e, --events <list>        Comma-separated events to wait on\n"
            "  -s, --set <name>           Event to trigger after work completes\n"
            "  -i, --trigger-start <name> Trigger one event once before waiting\n"
            "  -t, --timeout <value>      Wait timeout (us, ms, s, m; fractions allowed)\n"
            "  -w, --work <value>         Busy-work duration (us, ms, s, m)\n"
            "  -R, --runtime <value>      Maximum runtime (us, ms, s, m)\n"
            "  -c, --cpu <all|index>      Run on all CPUs or pin to one CPU index\n"
            "  -p, --policy <name>        Scheduler policy: other, fifo, rr\n"
            "  -r, --priority <value>     Scheduler priority\n"
            "  -n, --name <text>          Instance label shown in logs and stats\n"
            "  -v, --verbose              Print per-loop activity\n"
            "\n"
            "Accepted forms:\n"
            "  -o value\n"
            "  -o=value\n"
            "  --long value\n"
            "  --long=value\n"
            "\n"
            "Examples:\n"
            "  %s --timeout=2.5ms --work=250us --runtime=2s\n"
            "  %s --events=ping --set=pong --trigger-start=ping --timeout=1ms --work=200us --runtime=5s\n",
            program, program, program);
}

static int monotonic_now(struct timespec *ts)
{
    return clock_gettime(CLOCK_MONOTONIC, ts);
}

static int wallclock_now(clockid_t clock_id, struct timespec *ts)
{
    return clock_gettime(clock_id, ts);
}

static uint64_t timespec_to_ns(const struct timespec *ts)
{
    return ((uint64_t)ts->tv_sec * 1000000000ull) + (uint64_t)ts->tv_nsec;
}

static struct timespec ns_to_timespec(uint64_t ns)
{
    struct timespec ts;
    ts.tv_sec = (time_t)(ns / 1000000000ull);
    ts.tv_nsec = (long)(ns % 1000000000ull);
    return ts;
}

static uint64_t monotonic_now_ns(void)
{
    struct timespec ts;
    if (monotonic_now(&ts) != 0) {
        return 0;
    }
    return timespec_to_ns(&ts);
}

static bool add_duration_to_now(clockid_t clock_id, uint64_t duration_ns, struct timespec *deadline)
{
    struct timespec now;
    uint64_t total_ns;

    if (wallclock_now(clock_id, &now) != 0) {
        return false;
    }

    total_ns = timespec_to_ns(&now) + duration_ns;
    *deadline = ns_to_timespec(total_ns);
    return true;
}

static bool parse_duration_ns(const char *text, uint64_t *out_ns)
{
    char *endptr = NULL;
    double value;
    double multiplier = 1000000000.0;
    double result;

    if ((text == NULL) || (*text == '\0')) {
        return false;
    }

    errno = 0;
    value = strtod(text, &endptr);
    if ((errno != 0) || (endptr == text) || (value < 0.0)) {
        return false;
    }

    if ((*endptr == '\0') || (strcmp(endptr, "s") == 0)) {
        multiplier = 1000000000.0;
    } else if (strcmp(endptr, "ms") == 0) {
        multiplier = 1000000.0;
    } else if (strcmp(endptr, "us") == 0) {
        multiplier = 1000.0;
    } else if (strcmp(endptr, "m") == 0) {
        multiplier = 60.0 * 1000000000.0;
    } else {
        return false;
    }

    result = value * multiplier;
    if ((result < 0.0) || (result > (double)UINT64_MAX)) {
        return false;
    }

    *out_ns = (uint64_t)(result + 0.5);
    return true;
}

static bool parse_uint_value(const char *text, unsigned int *value_out)
{
    char *endptr = NULL;
    unsigned long value;

    if ((text == NULL) || (*text == '\0')) {
        return false;
    }

    errno = 0;
    value = strtoul(text, &endptr, 10);
    if ((errno != 0) || (endptr == text) || (*endptr != '\0') || (value > UINT32_MAX)) {
        return false;
    }

    *value_out = (unsigned int)value;
    return true;
}

static bool parse_int_value(const char *text, int *value_out)
{
    char *endptr = NULL;
    long value;

    if ((text == NULL) || (*text == '\0')) {
        return false;
    }

    errno = 0;
    value = strtol(text, &endptr, 10);
    if ((errno != 0) || (endptr == text) || (*endptr != '\0') || (value < INT32_MIN) || (value > INT32_MAX)) {
        return false;
    }

    *value_out = (int)value;
    return true;
}

static bool copy_name(const char *input, char *output, size_t output_size)
{
    size_t length;

    if ((input == NULL) || (*input == '\0')) {
        return false;
    }

    length = strlen(input);
    if (length >= output_size) {
        return false;
    }

    memcpy(output, input, length + 1u);
    return true;
}

static bool parse_event_list(const char *text, config_t *config)
{
    char buffer[512];
    char *saveptr = NULL;
    char *token = NULL;

    if ((text == NULL) || (*text == '\0')) {
        config->event_count = 0;
        return true;
    }

    if (strlen(text) >= sizeof(buffer)) {
        return false;
    }

    strcpy(buffer, text);
    config->event_count = 0;
    token = strtok_r(buffer, ",", &saveptr);
    while (token != NULL) {
        if (config->event_count >= THREADTEST_MAX_WAIT_EVENTS) {
            return false;
        }
        if (!copy_name(token, config->event_names[config->event_count], sizeof(config->event_names[config->event_count]))) {
            return false;
        }
        config->event_count++;
        token = strtok_r(NULL, ",", &saveptr);
    }

    return true;
}

static int parse_policy(const char *text, int *policy_out)
{
    if ((text == NULL) || (*text == '\0')) {
        return -1;
    }
    if (strcmp(text, "other") == 0) {
        *policy_out = SCHED_OTHER;
        return 0;
    }
    if (strcmp(text, "fifo") == 0) {
        *policy_out = SCHED_FIFO;
        return 0;
    }
    if (strcmp(text, "rr") == 0) {
        *policy_out = SCHED_RR;
        return 0;
    }
    return -1;
}

static void default_config(config_t *config)
{
    memset(config, 0, sizeof(*config));
    if (!copy_name("threadtest", config->instance_name, sizeof(config->instance_name))) {
        config->instance_name[0] = '\0';
    }
    config->sched_policy = SCHED_OTHER;
    config->timeout_ns = 1000000ull;
    config->work_ns = 100000ull;
    config->runtime_ns = 1000000000ull;
}

static const char *next_option_value(const char *option, const char *attached_value, int argc, char **argv, int *index)
{
    if ((attached_value != NULL) && (*attached_value != '\0')) {
        return attached_value;
    }

    if ((*index + 1) >= argc) {
        fprintf(stderr, "Missing value for option '%s'.\n", option);
        return NULL;
    }

    (*index)++;
    return argv[*index];
}

static int parse_args(int argc, char **argv, config_t *config)
{
    int index;

    if (argc <= 1) {
        print_usage(stdout, argv[0]);
        return 1;
    }

    default_config(config);

    for (index = 1; index < argc; ++index) {
        const char *arg = argv[index];
        const char *value = NULL;
        const char *equals = NULL;
        const char *option = NULL;
        char option_buffer[32];

        if ((strcmp(arg, "help") == 0) || (strcmp(arg, "--help") == 0) || (strcmp(arg, "-h") == 0)) {
            print_usage(stdout, argv[0]);
            return 1;
        }

        if (arg[0] != '-') {
            fprintf(stderr, "Unexpected argument '%s'. Use --help for usage.\n", arg);
            return -1;
        }

        equals = strchr(arg, '=');
        option = arg;
        if (equals != NULL) {
            size_t option_length = (size_t)(equals - arg);
            if (option_length >= sizeof(option_buffer)) {
                fprintf(stderr, "Option name too long: '%s'.\n", arg);
                return -1;
            }
            memcpy(option_buffer, arg, option_length);
            option_buffer[option_length] = '\0';
            option = option_buffer;
            value = equals + 1;
        }

        if ((strcmp(option, "--events") == 0) || (strcmp(option, "-e") == 0)) {
            value = next_option_value(option, value, argc, argv, &index);
            if ((value == NULL) || !parse_event_list(value, config)) {
                fprintf(stderr, "Invalid event list '%s'. Use comma-separated names (max %u events).\n",
                        (value != NULL) ? value : "", THREADTEST_MAX_WAIT_EVENTS);
                return -1;
            }
        } else if ((strcmp(option, "--set") == 0) || (strcmp(option, "-s") == 0)) {
            value = next_option_value(option, value, argc, argv, &index);
            if ((value == NULL) || !copy_name(value, config->set_event, sizeof(config->set_event))) {
                fprintf(stderr, "Invalid event name for --set.\n");
                return -1;
            }
            config->has_set_event = true;
        } else if ((strcmp(option, "--trigger-start") == 0) || (strcmp(option, "-i") == 0)) {
            value = next_option_value(option, value, argc, argv, &index);
            if ((value == NULL) || !copy_name(value, config->trigger_start_event, sizeof(config->trigger_start_event))) {
                fprintf(stderr, "Invalid event name for --trigger-start.\n");
                return -1;
            }
            config->has_trigger_start_event = true;
        } else if ((strcmp(option, "--timeout") == 0) || (strcmp(option, "-t") == 0)) {
            value = next_option_value(option, value, argc, argv, &index);
            if ((value == NULL) || !parse_duration_ns(value, &config->timeout_ns) || (config->timeout_ns == 0u)) {
                fprintf(stderr, "Invalid timeout '%s'. Use a positive value like 500us, 2ms, 1.5s, or 1m.\n",
                        (value != NULL) ? value : "");
                return -1;
            }
        } else if ((strcmp(option, "--work") == 0) || (strcmp(option, "-w") == 0)) {
            value = next_option_value(option, value, argc, argv, &index);
            if ((value == NULL) || !parse_duration_ns(value, &config->work_ns)) {
                fprintf(stderr, "Invalid work duration '%s'.\n", (value != NULL) ? value : "");
                return -1;
            }
        } else if ((strcmp(option, "--runtime") == 0) || (strcmp(option, "-R") == 0)) {
            value = next_option_value(option, value, argc, argv, &index);
            if ((value == NULL) || !parse_duration_ns(value, &config->runtime_ns) || (config->runtime_ns == 0u)) {
                fprintf(stderr, "Invalid runtime '%s'.\n", (value != NULL) ? value : "");
                return -1;
            }
        } else if ((strcmp(option, "--cpu") == 0) || (strcmp(option, "-c") == 0)) {
            value = next_option_value(option, value, argc, argv, &index);
            if (value == NULL) {
                return -1;
            }
            if (strcmp(value, "all") == 0) {
                config->pin_to_cpu = false;
            } else {
                if (!parse_uint_value(value, &config->cpu_index)) {
                    fprintf(stderr, "Invalid CPU value '%s'. Use 'all' or a zero-based CPU index.\n", value);
                    return -1;
                }
                config->pin_to_cpu = true;
            }
        } else if ((strcmp(option, "--policy") == 0) || (strcmp(option, "-p") == 0)) {
            value = next_option_value(option, value, argc, argv, &index);
            if ((value == NULL) || (parse_policy(value, &config->sched_policy) != 0)) {
                fprintf(stderr, "Invalid policy '%s'. Use other, fifo, or rr.\n", (value != NULL) ? value : "");
                return -1;
            }
        } else if ((strcmp(option, "--priority") == 0) || (strcmp(option, "-r") == 0)) {
            value = next_option_value(option, value, argc, argv, &index);
            if ((value == NULL) || !parse_int_value(value, &config->sched_priority)) {
                fprintf(stderr, "Invalid priority '%s'. Use an integer value.\n", (value != NULL) ? value : "");
                return -1;
            }
        } else if ((strcmp(option, "--name") == 0) || (strcmp(option, "-n") == 0)) {
            value = next_option_value(option, value, argc, argv, &index);
            if ((value == NULL) || !copy_name(value, config->instance_name, sizeof(config->instance_name))) {
                fprintf(stderr, "Invalid instance name '%s'.\n", (value != NULL) ? value : "");
                return -1;
            }
        } else if ((strcmp(option, "--verbose") == 0) || (strcmp(option, "-v") == 0)) {
            config->verbose = true;
        } else {
            fprintf(stderr, "Unknown option '%s'. Use --help for usage.\n", option);
            return -1;
        }
    }

    return 0;
}

static os_kind_t detect_os(const char **os_name_out)
{
    static char os_name[THREADTEST_OS_NAME_MAX];
    struct utsname info;

    if (uname(&info) != 0) {
        snprintf(os_name, sizeof(os_name), "Unknown");
        *os_name_out = os_name;
        return OS_KIND_UNKNOWN;
    }

    if (strcmp(info.sysname, "Linux") == 0) {
        snprintf(os_name, sizeof(os_name), "Linux (%.*s)", (int)THREADTEST_RELEASE_TEXT_MAX, info.release);
        *os_name_out = os_name;
        return OS_KIND_LINUX;
    }

    if (strcmp(info.sysname, "QNX") == 0) {
        if (strncmp(info.release, "7.1", 3) == 0) {
            snprintf(os_name, sizeof(os_name), "QNX 7.1 (%.*s)", (int)THREADTEST_RELEASE_TEXT_MAX, info.release);
            *os_name_out = os_name;
            return OS_KIND_QNX_71;
        }
        if (strncmp(info.release, "8.", 2) == 0) {
            snprintf(os_name, sizeof(os_name), "QNX 8 (%.*s)", (int)THREADTEST_RELEASE_TEXT_MAX, info.release);
            *os_name_out = os_name;
            return OS_KIND_QNX_8;
        }
        snprintf(os_name, sizeof(os_name), "QNX (%.*s)", (int)THREADTEST_RELEASE_TEXT_MAX, info.release);
        *os_name_out = os_name;
        return OS_KIND_QNX_OTHER;
    }

    snprintf(os_name, sizeof(os_name), "%.30s (%.*s)", info.sysname, (int)THREADTEST_RELEASE_TEXT_MAX, info.release);
    *os_name_out = os_name;
    return OS_KIND_UNKNOWN;
}

static int lock_hub(pthread_mutex_t *mutex)
{
    int result = pthread_mutex_lock(mutex);
    return result;
}

static int unlock_hub(pthread_mutex_t *mutex)
{
    int result = pthread_mutex_unlock(mutex);
    return result;
}

static int init_shared_hub(shared_hub_t *hub)
{
    pthread_mutexattr_t mutex_attr;
    pthread_condattr_t cond_attr;
    int result;
    int monotonic_supported = 0;

    memset(hub, 0, sizeof(*hub));

    result = pthread_mutexattr_init(&mutex_attr);
    if (result != 0) {
        return result;
    }
    result = pthread_mutexattr_setpshared(&mutex_attr, PTHREAD_PROCESS_SHARED);
    if (result != 0) {
        pthread_mutexattr_destroy(&mutex_attr);
        return result;
    }
    result = pthread_mutex_init(&hub->mutex, &mutex_attr);
    pthread_mutexattr_destroy(&mutex_attr);
    if (result != 0) {
        return result;
    }

    result = pthread_condattr_init(&cond_attr);
    if (result != 0) {
        pthread_mutex_destroy(&hub->mutex);
        return result;
    }
    result = pthread_condattr_setpshared(&cond_attr, PTHREAD_PROCESS_SHARED);
    if (result != 0) {
        pthread_condattr_destroy(&cond_attr);
        pthread_mutex_destroy(&hub->mutex);
        return result;
    }
#if defined(CLOCK_MONOTONIC)
    if (pthread_condattr_setclock(&cond_attr, CLOCK_MONOTONIC) == 0) {
        monotonic_supported = 1;
    }
#endif
    result = pthread_cond_init(&hub->cond, &cond_attr);
    pthread_condattr_destroy(&cond_attr);
    if (result != 0) {
        pthread_mutex_destroy(&hub->mutex);
        return result;
    }

    hub->cond_clock_id = (uint32_t)(monotonic_supported ? CLOCK_MONOTONIC : CLOCK_REALTIME);
    hub->event_count = 0;
    hub->version = THREADTEST_VERSION;
    hub->magic = THREADTEST_MAGIC;
    return 0;
}

static shared_hub_t *open_shared_hub(void)
{
    int fd;
    bool creator = false;
    shared_hub_t *hub = NULL;
    int attempts;

    fd = shm_open(THREADTEST_SHM_NAME, O_RDWR | O_CREAT | O_EXCL, 0666);
    if (fd >= 0) {
        creator = true;
    } else if (errno == EEXIST) {
        fd = shm_open(THREADTEST_SHM_NAME, O_RDWR, 0666);
    }

    if (fd < 0) {
        fprintf(stderr, "shm_open('%s') failed: %s\n", THREADTEST_SHM_NAME, strerror(errno));
        return NULL;
    }

    if (ftruncate(fd, (off_t)sizeof(shared_hub_t)) != 0) {
        fprintf(stderr, "ftruncate shared hub failed: %s\n", strerror(errno));
        close(fd);
        if (creator) {
            shm_unlink(THREADTEST_SHM_NAME);
        }
        return NULL;
    }

    hub = mmap(NULL, sizeof(shared_hub_t), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (hub == MAP_FAILED) {
        fprintf(stderr, "mmap shared hub failed: %s\n", strerror(errno));
        if (creator) {
            shm_unlink(THREADTEST_SHM_NAME);
        }
        return NULL;
    }

    if (creator) {
        int init_result = init_shared_hub(hub);
        if (init_result != 0) {
            fprintf(stderr, "Initializing shared hub failed: %s\n", strerror(init_result));
            munmap(hub, sizeof(shared_hub_t));
            shm_unlink(THREADTEST_SHM_NAME);
            return NULL;
        }
        return hub;
    }

    for (attempts = 0; attempts < 500; ++attempts) {
        if ((hub->magic == THREADTEST_MAGIC) && (hub->version == THREADTEST_VERSION)) {
            return hub;
        }
        usleep(10000);
    }

    fprintf(stderr, "Timed out waiting for shared hub initialization.\n");
    munmap(hub, sizeof(shared_hub_t));
    return NULL;
}

static int find_event_index_locked(shared_hub_t *hub, const char *name)
{
    uint32_t index;
    for (index = 0; index < hub->event_count; ++index) {
        if ((hub->events[index].in_use != 0u) && (strncmp(hub->events[index].name, name, THREADTEST_EVENT_NAME_MAX) == 0)) {
            return (int)index;
        }
    }
    return -1;
}

static int get_or_create_event_index(shared_hub_t *hub, const char *name)
{
    int index = find_event_index_locked(hub, name);

    if (index >= 0) {
        return index;
    }

    if (hub->event_count >= THREADTEST_MAX_EVENTS) {
        return -1;
    }

    index = (int)hub->event_count;
    memset(&hub->events[index], 0, sizeof(hub->events[index]));
    memcpy(hub->events[index].name, name, strlen(name) + 1u);
    hub->events[index].in_use = 1u;
    hub->event_count++;
    return index;
}

static int resolve_events(app_t *app)
{
    int result;
    size_t index;

    result = lock_hub(&app->hub->mutex);
    if (result != 0) {
        fprintf(stderr, "Failed to lock shared hub: %s\n", strerror(result));
        return -1;
    }

    for (index = 0; index < app->config.event_count; ++index) {
        int event_index = get_or_create_event_index(app->hub, app->config.event_names[index]);
        if (event_index < 0) {
            unlock_hub(&app->hub->mutex);
            fprintf(stderr, "Unable to allocate shared event '%s'.\n", app->config.event_names[index]);
            return -1;
        }
        app->wait_event_indices[index] = event_index;
        app->seen_generations[index] = app->hub->events[event_index].generation;
    }

    if (app->config.has_set_event) {
        app->set_event_index = get_or_create_event_index(app->hub, app->config.set_event);
        if (app->set_event_index < 0) {
            unlock_hub(&app->hub->mutex);
            fprintf(stderr, "Unable to allocate shared event '%s'.\n", app->config.set_event);
            return -1;
        }
    } else {
        app->set_event_index = -1;
    }

    if (app->config.has_trigger_start_event) {
        app->trigger_start_index = get_or_create_event_index(app->hub, app->config.trigger_start_event);
        if (app->trigger_start_index < 0) {
            unlock_hub(&app->hub->mutex);
            fprintf(stderr, "Unable to allocate shared event '%s'.\n", app->config.trigger_start_event);
            return -1;
        }
    } else {
        app->trigger_start_index = -1;
    }

    result = unlock_hub(&app->hub->mutex);
    if (result != 0) {
        fprintf(stderr, "Failed to unlock shared hub: %s\n", strerror(result));
        return -1;
    }

    return 0;
}

static int trigger_event_locked(app_t *app, int event_index)
{
    if (event_index < 0) {
        return 0;
    }

    app->hub->events[event_index].generation++;
    app->hub->events[event_index].trigger_count++;
    app->stats.triggers_sent++;
    return pthread_cond_broadcast(&app->hub->cond);
}

static int trigger_event(app_t *app, int event_index)
{
    int result = lock_hub(&app->hub->mutex);
    if (result != 0) {
        fprintf(stderr, "Failed to lock shared hub for trigger: %s\n", strerror(result));
        return -1;
    }

    result = trigger_event_locked(app, event_index);
    if (result != 0) {
        fprintf(stderr, "Failed to signal shared event: %s\n", strerror(result));
        unlock_hub(&app->hub->mutex);
        return -1;
    }

    result = unlock_hub(&app->hub->mutex);
    if (result != 0) {
        fprintf(stderr, "Failed to unlock shared hub after trigger: %s\n", strerror(result));
        return -1;
    }
    return 0;
}

static bool consume_triggered_events_locked(app_t *app)
{
    bool triggered = false;
    size_t index;

    for (index = 0; index < app->config.event_count; ++index) {
        int event_index = app->wait_event_indices[index];
        uint64_t current_generation = app->hub->events[event_index].generation;
        if (current_generation != app->seen_generations[index]) {
            app->stats.triggers_received += current_generation - app->seen_generations[index];
            app->seen_generations[index] = current_generation;
            triggered = true;
        }
    }

    return triggered;
}

static int apply_affinity(const app_t *app)
{
    if (!app->config.pin_to_cpu) {
        return 0;
    }

#if defined(__linux__)
    cpu_set_t cpu_set;
    int result;
    CPU_ZERO(&cpu_set);
    CPU_SET(app->config.cpu_index, &cpu_set);
    result = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);
    if (result != 0) {
        fprintf(stderr, "Failed to pin worker to CPU %u: %s\n",
                app->config.cpu_index, strerror(result));
        return -1;
    }
    return 0;
#elif defined(__QNXNTO__)
    {
        struct _thread_runmask runmask;
        memset(&runmask, 0, sizeof(runmask));
        runmask.id = 0u;
        if (app->config.cpu_index >= 64u) {
            fprintf(stderr, "QNX runmask backend supports CPU indices 0-63 only.\n");
            return -1;
        }
        runmask.runmask = ((uint64_t)1) << app->config.cpu_index;
        runmask.inherit = 0u;
        if (ThreadCtl(_NTO_TCTL_RUNMASK, &runmask) == -1) {
            fprintf(stderr, "Failed to pin worker to CPU %u: %s\n",
                    app->config.cpu_index, strerror(errno));
            return -1;
        }
        return 0;
    }
#else
    fprintf(stderr, "CPU pinning is not implemented on this platform.\n");
    return -1;
#endif
}

static int apply_scheduler(const app_t *app)
{
    struct sched_param parameter;
    int min_priority;
    int max_priority;
    int result;

    memset(&parameter, 0, sizeof(parameter));

    min_priority = sched_get_priority_min(app->config.sched_policy);
    max_priority = sched_get_priority_max(app->config.sched_policy);
    if ((min_priority == -1) || (max_priority == -1)) {
        perror("sched_get_priority_*");
        return -1;
    }

    if ((app->config.sched_priority < min_priority) || (app->config.sched_priority > max_priority)) {
        fprintf(stderr, "Priority %d is outside the valid range [%d, %d] for the selected policy.\n",
                app->config.sched_priority, min_priority, max_priority);
        return -1;
    }

    parameter.sched_priority = app->config.sched_priority;
    result = pthread_setschedparam(pthread_self(), app->config.sched_policy, &parameter);
    if (result != 0) {
        fprintf(stderr, "Failed to set worker scheduling policy/priority: %s\n", strerror(result));
        return -1;
    }

    return 0;
}

static void busy_work(uint64_t duration_ns)
{
    uint64_t start_ns;
    uint64_t end_ns;
    volatile uint64_t sink = g_work_sink;

    start_ns = monotonic_now_ns();
    end_ns = start_ns + duration_ns;
    while (monotonic_now_ns() < end_ns) {
        sink = (sink * 1664525u) + 1013904223u;
    }
    g_work_sink = sink;
}

static void print_verbose(const config_t *config, const char *message)
{
    if (config->verbose) {
        printf("[%s] %s\n", config->instance_name, message);
    }
}

static void *worker_main(void *opaque)
{
    app_t *app = (app_t *)opaque;
    uint64_t start_ns = monotonic_now_ns();

    if (apply_affinity(app) != 0) {
        app->result_code = 2;
        return NULL;
    }

    if (apply_scheduler(app) != 0) {
        app->result_code = 2;
        return NULL;
    }

    for (;;) {
        int result;
        bool triggered = false;
        bool timed_out = false;

        if (g_signal_stop != 0) {
            break;
        }
        if ((app->config.runtime_ns > 0u) && ((monotonic_now_ns() - start_ns) >= app->config.runtime_ns)) {
            break;
        }

        app->stats.loop_iterations++;
        app->stats.wait_calls++;

        result = lock_hub(&app->hub->mutex);
        if (result != 0) {
            fprintf(stderr, "Failed to lock shared hub in worker: %s\n", strerror(result));
            app->result_code = 2;
            return NULL;
        }

        triggered = consume_triggered_events_locked(app);
        if (!triggered) {
            struct timespec deadline;
            clockid_t clock_id = (clockid_t)app->hub->cond_clock_id;

            if (!add_duration_to_now(clock_id, app->config.timeout_ns, &deadline)) {
                fprintf(stderr, "Failed to compute wait deadline.\n");
                unlock_hub(&app->hub->mutex);
                app->result_code = 2;
                return NULL;
            }

            result = pthread_cond_timedwait(&app->hub->cond, &app->hub->mutex, &deadline);
            if ((result != 0) && (result != ETIMEDOUT)) {
                fprintf(stderr, "pthread_cond_timedwait failed: %s\n", strerror(result));
                unlock_hub(&app->hub->mutex);
                app->result_code = 2;
                return NULL;
            }
            if (result == ETIMEDOUT) {
                timed_out = true;
            }
            triggered = consume_triggered_events_locked(app);
        }

        result = unlock_hub(&app->hub->mutex);
        if (result != 0) {
            fprintf(stderr, "Failed to unlock shared hub in worker: %s\n", strerror(result));
            app->result_code = 2;
            return NULL;
        }

        if (!triggered && timed_out) {
            app->stats.timeouts++;
            print_verbose(&app->config, "timeout");
        } else if (triggered) {
            print_verbose(&app->config, "trigger received");
        }

        {
            uint64_t work_start_ns = monotonic_now_ns();
            busy_work(app->config.work_ns);
            app->stats.work_calls++;
            app->stats.work_ns_total += monotonic_now_ns() - work_start_ns;
        }

        if (app->set_event_index >= 0) {
            if (trigger_event(app, app->set_event_index) != 0) {
                app->result_code = 2;
                return NULL;
            }
            print_verbose(&app->config, "trigger sent");
        }
    }

    app->result_code = 0;
    return NULL;
}

static void print_stats(const app_t *app)
{
    printf("\nStatistics for %s\n", app->config.instance_name);
    printf("  detected OS:        %s\n", app->os_name);
    printf("  loop iterations:    %" PRIu64 "\n", app->stats.loop_iterations);
    printf("  wait calls:         %" PRIu64 "\n", app->stats.wait_calls);
    printf("  triggers received:  %" PRIu64 "\n", app->stats.triggers_received);
    printf("  triggers sent:      %" PRIu64 "\n", app->stats.triggers_sent);
    printf("  timeouts:           %" PRIu64 "\n", app->stats.timeouts);
    printf("  work calls:         %" PRIu64 "\n", app->stats.work_calls);
    printf("  work total (us):    %" PRIu64 "\n", (uint64_t)(app->stats.work_ns_total / 1000ull));
}

int main(int argc, char **argv)
{
    app_t app;
    int parse_result;
    pthread_t worker;
    int join_result;
    struct sigaction action;

    memset(&app, 0, sizeof(app));

    parse_result = parse_args(argc, argv, &app.config);
    if (parse_result > 0) {
        return 0;
    }
    if (parse_result < 0) {
        return 2;
    }

    app.os_kind = detect_os(&app.os_name);
    (void)app.os_kind;
    printf("Detected OS: %s\n", app.os_name);

    app.hub = open_shared_hub();
    if (app.hub == NULL) {
        return 2;
    }

    if (resolve_events(&app) != 0) {
        munmap(app.hub, sizeof(*app.hub));
        return 2;
    }

    memset(&action, 0, sizeof(action));
    action.sa_handler = handle_signal;
    sigemptyset(&action.sa_mask);
    sigaction(SIGINT, &action, NULL);
    sigaction(SIGTERM, &action, NULL);

    if (app.trigger_start_index >= 0) {
        if (trigger_event(&app, app.trigger_start_index) != 0) {
            munmap(app.hub, sizeof(*app.hub));
            return 2;
        }
    }

    {
        int create_result = pthread_create(&worker, NULL, worker_main, &app);
        if (create_result != 0) {
            fprintf(stderr, "pthread_create failed: %s\n", strerror(create_result));
            munmap(app.hub, sizeof(*app.hub));
            return 2;
        }
    }

    join_result = pthread_join(worker, NULL);
    if (join_result != 0) {
        fprintf(stderr, "pthread_join failed: %s\n", strerror(join_result));
        munmap(app.hub, sizeof(*app.hub));
        return 2;
    }

    print_stats(&app);
    munmap(app.hub, sizeof(*app.hub));
    return app.result_code;
}
