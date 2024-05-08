#include <execinfo.h>
#include <unistd.h>
#include <signal.h>

#include "postgres.h"
#include "miscadmin.h"
#include "parser/analyze.h"
#include "utils/guc.h"
#include "tcop/utility.h"
#include "executor/executor.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define MAX_BACK_TRACE_DEPTH    100
#define SKIP_FRAMES             3

/* pg module functions */
void _PG_init(void);
void _PG_fini(void);

PG_FUNCTION_INFO_V1(pg_backtrace_init);

/* Disabled for user safety */
/* PG_FUNCTION_INFO_V1(pg_backtrace_force_crash); */

static int backtrace_level = FATAL;
static ErrorContextCallback backtrace_callback;
static ExecutorRun_hook_type prev_executor_run_hook;
static ProcessUtility_hook_type prev_utility_hook;
static post_parse_analyze_hook_type prev_post_parse_analyze_hook;
static bool inside_signal_handler = false;
static pqsigfunc signal_handlers[_NSIG];
static bool already_printed = false;

static void
backtrace_dump_stack(void)
{
    void* back_trace[MAX_BACK_TRACE_DEPTH];
    int depth = backtrace(back_trace, MAX_BACK_TRACE_DEPTH);
    char** stack = backtrace_symbols(back_trace+SKIP_FRAMES, depth-SKIP_FRAMES);
	if (stack != NULL)
	{
		int i;
		for (i = 0; i < depth-SKIP_FRAMES; i++) {
			errcontext_msg("\t%s", stack[i]);
		}
		free(stack);
	}
}

static void
backtrace_callback_function(void* arg)
{
	if (inside_signal_handler)
	{
		if (already_printed)
			exit(1);

		already_printed = true;
		backtrace_dump_stack();
	}
	else
	{
		MemoryContext oldcontext, tmpcontext;
		int elevel;
		ErrorData *edata;

		tmpcontext = AllocSetContextCreate(CurrentMemoryContext, "pg_backtrace temporary context", ALLOCSET_DEFAULT_SIZES);
		oldcontext = MemoryContextSwitchTo(tmpcontext);
		edata = CopyErrorData();
		elevel = edata->elevel;
		MemoryContextSwitchTo(oldcontext);
		MemoryContextReset(tmpcontext);

		if (elevel >= backtrace_level)
			backtrace_dump_stack();
	}
}

static void backtrace_register_error_callback(void)
{
	ErrorContextCallback* esp;
	for (esp = error_context_stack; esp != NULL && esp != &backtrace_callback; esp = esp->previous);
	if (esp == NULL)
	{
		backtrace_callback.callback = backtrace_callback_function;
		backtrace_callback.previous = error_context_stack;
		error_context_stack = &backtrace_callback;
	}
}

static void
backtrace_executor_run_hook(QueryDesc *queryDesc,
							ScanDirection direction,
							uint64 count,
							bool execute_once)
{
	backtrace_register_error_callback();
	if (prev_executor_run_hook)
		(*prev_executor_run_hook)(queryDesc, direction, count, execute_once);
	else
		standard_ExecutorRun(queryDesc, direction, count, execute_once);
}

static void backtrace_utility_hook(PlannedStmt *pstmt, const char *queryString,
							bool readOnlyTree, ProcessUtilityContext context,
							ParamListInfo params,
								   QueryEnvironment *queryEnv,
								   DestReceiver *dest, QueryCompletion *completionTag)
{
	backtrace_register_error_callback();
	if (prev_utility_hook)
		(*prev_utility_hook)(pstmt, queryString, readOnlyTree,
							 context, params, queryEnv,
							 dest, completionTag);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree,
								context, params, queryEnv,
								dest, completionTag);
}

static void backtrace_post_parse_analyze_hook(ParseState *pstate, Query *query, JumbleState* jstate)
{
	backtrace_register_error_callback();
	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query, jstate);
}


static void
backtrace_handler(SIGNAL_ARGS)
{
	inside_signal_handler = true;
	elog(LOG, "Caught signal %d", postgres_signal_arg);
	inside_signal_handler = false;
	if (signal_handlers[postgres_signal_arg])
	{
		signal_handlers[postgres_signal_arg](postgres_signal_arg);
	}
}

static const struct config_enum_entry backtrace_level_options[] =
{
	{"debug5", DEBUG5, false},
	{"debug4", DEBUG4, false},
	{"debug3", DEBUG3, false},
	{"debug2", DEBUG2, false},
	{"debug1", DEBUG1, false},
	{"debug", DEBUG2, true},
	{"log", LOG, false},
	{"info", INFO, false},
	{"notice", NOTICE, false},
	{"warning", WARNING, false},
	{"error", ERROR, false},
	{"fatal", FATAL, false},
	{"panic", PANIC, false},
	{NULL, 0, false}
};

void _PG_init(void)
{
	signal_handlers[SIGSEGV] = pqsignal(SIGSEGV, backtrace_handler);
	signal_handlers[SIGBUS] = pqsignal(SIGBUS, backtrace_handler);
	signal_handlers[SIGFPE] = pqsignal(SIGFPE, backtrace_handler);
	signal_handlers[SIGINT] = pqsignal(SIGINT, backtrace_handler);
	prev_executor_run_hook = ExecutorRun_hook;
	ExecutorRun_hook = backtrace_executor_run_hook;

	prev_utility_hook = ProcessUtility_hook;
	ProcessUtility_hook = backtrace_utility_hook;

	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = backtrace_post_parse_analyze_hook;

	DefineCustomEnumVariable("pg_backtrace.level",
							 "Set error level for dumping backtrace",
							 NULL,
							 &backtrace_level,
							 FATAL,
							 backtrace_level_options,
							 PGC_USERSET, 0, NULL, NULL, NULL);
}

void _PG_fini(void)
{
	ExecutorRun_hook = prev_executor_run_hook;
	ProcessUtility_hook = prev_utility_hook;
	post_parse_analyze_hook = prev_post_parse_analyze_hook;

	pqsignal(SIGSEGV, signal_handlers[SIGSEGV]);
	pqsignal(SIGBUS,  signal_handlers[SIGBUS]);
	pqsignal(SIGFPE,  signal_handlers[SIGFPE]);
	pqsignal(SIGINT,  signal_handlers[SIGINT]);
}

Datum pg_backtrace_init(PG_FUNCTION_ARGS);
/* Disabled for user safety */
/* Datum pg_backtrace_force_crash(PG_FUNCTION_ARGS); */

Datum pg_backtrace_init(PG_FUNCTION_ARGS)
{
	PG_RETURN_VOID();
}

/* Disabled for user safety */
/*
Datum pg_backtrace_force_crash(PG_FUNCTION_ARGS)
{
	volatile int *n = 0;
	*n = 0;
	PG_RETURN_VOID();
}
*/
