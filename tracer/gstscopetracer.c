/*
 * gstscope tracer
 *
 * GST_TRACERS=gstscope                         -> default socket, 1000 ms
 * GST_TRACERS="gstscope(interval=500,socket=/tmp/gstscope.sock)"
 *
 * Collects, per pipeline in the process:
 *   - per src pad: buffers, bytes, flow errors           (-> fps / kbps per link)
 *   - per element: processing time (sink-in -> src-out)  (approximate, FIFO matched)
 *   - per sink:    push time into the sink (includes clock sync)
 *   - queue / queue2 fill levels
 *   - QoS dropped counters, ERROR / WARNING / EOS / state messages
 *   - topology (leaf elements + links, ghost pads resolved)
 *
 * Everything is sent as one JSON object per datagram over a unix SOCK_DGRAM
 * socket with MSG_DONTWAIT, so the traced application never blocks and does not
 * care whether the gstscope daemon is running.
 *
 * Works with GStreamer >= 1.16 (Jetson JetPack 5, Orange Pi Ubuntu 22.04, ...).
 *
 * SPDX-License-Identifier: MIT
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <gst/gst.h>
#include <gst/gsttracer.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#ifndef GSTSCOPE_VERSION
#define GSTSCOPE_VERSION "0.1.0"
#endif

#define DEFAULT_SOCKET   "/run/gstscope/gstscope.sock"
#define DEFAULT_INTERVAL 1000
#define FIFO_MAX         256
#define CAPS_MAX         400
#define TOPO_EVERY       5   /* resend topology every N reports even if unchanged */

GST_DEBUG_CATEGORY_STATIC (gst_scope_debug);
#define GST_CAT_DEFAULT gst_scope_debug

/* ------------------------------------------------------------------------- */
/* per-object statistics (attached as qdata)                                 */

typedef struct
{
  guint64 buffers, bytes;          /* cumulative */
  guint64 last_buffers, last_bytes;/* at last report */
  guint64 flow_errors;             /* cumulative non-OK (except FLUSHING) */
  gint last_flow;                  /* last non-OK flow return */
  GstClockTime last_pts;
  GstClockTime push_start;         /* pad-push-pre ts, for sink timing */

  /* cache: leaf element receiving our buffers */
  gint cache_gen;
  GstElement *downstream;          /* owned ref, may be NULL */
  gboolean downstream_is_sink;
} PadStats;

typedef struct
{
  guint64 ring[FIFO_MAX];          /* arrival timestamps, FIFO order */
  guint head, len;
  guint64 proc_sum, proc_cnt, proc_max;      /* since last report */
  guint64 sink_sum, sink_cnt, sink_max;      /* since last report */
  guint64 qos_dropped, qos_processed;        /* latest cumulative from QOS msg */
  guint64 fifo_overflows;
} ElemStats;

/* ------------------------------------------------------------------------- */

#define GST_TYPE_SCOPE_TRACER (gst_scope_tracer_get_type ())
G_DECLARE_FINAL_TYPE (GstScopeTracer, gst_scope_tracer, GST, SCOPE_TRACER, GstTracer)

struct _GstScopeTracer
{
  GstTracer parent;

  gchar *socket_path;
  guint interval_ms;

  gint sock;
  struct sockaddr_un addr;
  socklen_t addr_len;
  gboolean warned_send;

  GMutex lock;                     /* protects stats, pipelines */
  GPtrArray *pipelines;            /* GWeakRef* */

  gint topo_gen;                   /* atomic */

  GThread *thread;
  GMutex thread_lock;
  GCond thread_cond;
  gboolean stop;

  gint pid;
  gchar proc_name[64];
};

G_DEFINE_TYPE (GstScopeTracer, gst_scope_tracer, GST_TYPE_TRACER);

static GQuark pad_quark, elem_quark;

/* ------------------------------------------------------------------------- */
/* helpers                                                                   */

static void
pad_stats_free (gpointer data)
{
  PadStats *ps = data;
  if (ps->downstream)
    gst_object_unref (ps->downstream);
  g_free (ps);
}

static void
elem_stats_free (gpointer data)
{
  g_free (data);
}

static PadStats *
get_pad_stats (GstPad * pad)
{
  PadStats *ps = g_object_get_qdata (G_OBJECT (pad), pad_quark);
  if (G_UNLIKELY (!ps)) {
    ps = g_new0 (PadStats, 1);
    ps->last_pts = GST_CLOCK_TIME_NONE;
    ps->cache_gen = -1;
    g_object_set_qdata_full (G_OBJECT (pad), pad_quark, ps, pad_stats_free);
  }
  return ps;
}

static ElemStats *
get_elem_stats (GstElement * el)
{
  ElemStats *es = g_object_get_qdata (G_OBJECT (el), elem_quark);
  if (G_UNLIKELY (!es)) {
    es = g_new0 (ElemStats, 1);
    g_object_set_qdata_full (G_OBJECT (el), elem_quark, es, elem_stats_free);
  }
  return es;
}

/* Proxy pads that are not ghost pads are the "internal" pads of ghost pads. */
static inline gboolean
is_internal_proxy (GstPad * pad)
{
  return GST_IS_PROXY_PAD (pad) && !GST_IS_GHOST_PAD (pad);
}

/* Follow a peer pad through ghost / proxy pads until a pad on a leaf element.
 * Takes ownership of @pad, returns a new ref or NULL. */
static GstPad *
resolve_leaf_pad (GstPad * pad)
{
  gint guard = 0;
  while (pad && guard++ < 32) {
    GstPad *next = NULL;
    if (GST_IS_GHOST_PAD (pad)) {
      /* ghost pad on a bin; go inside */
      GstProxyPad *internal = gst_proxy_pad_get_internal (GST_PROXY_PAD (pad));
      if (internal) {
        next = gst_pad_get_peer (GST_PAD (internal));
        gst_object_unref (internal);
      }
    } else if (is_internal_proxy (pad)) {
      /* internal pad of a ghost pad; go outside */
      GstProxyPad *ghost = gst_proxy_pad_get_internal (GST_PROXY_PAD (pad));
      if (ghost) {
        next = gst_pad_get_peer (GST_PAD (ghost));
        gst_object_unref (ghost);
      }
    } else {
      return pad;
    }
    gst_object_unref (pad);
    pad = next;
  }
  if (pad)
    gst_object_unref (pad);
  return NULL;
}

static void
json_str (GString * s, const gchar * str)
{
  const guchar *p;
  g_string_append_c (s, '"');
  if (str) {
    for (p = (const guchar *) str; *p; p++) {
      switch (*p) {
        case '"':  g_string_append (s, "\\\""); break;
        case '\\': g_string_append (s, "\\\\"); break;
        case '\n': g_string_append (s, "\\n"); break;
        case '\r': g_string_append (s, "\\r"); break;
        case '\t': g_string_append (s, "\\t"); break;
        default:
          if (*p < 0x20)
            g_string_append_printf (s, "\\u%04x", *p);
          else
            g_string_append_c (s, *p);
      }
    }
  }
  g_string_append_c (s, '"');
}

/* Element id = path below the pipeline, e.g. "decodebin0/h264parse0" */
static gchar *
element_id (GstElement * el, GstElement * pipeline)
{
  GString *s = g_string_new (NULL);
  GstObject *o = GST_OBJECT (el);
  GList *names = NULL, *l;

  GST_OBJECT_LOCK (o);
  names = g_list_prepend (names, g_strdup (GST_OBJECT_NAME (o)));
  GST_OBJECT_UNLOCK (o);

  o = gst_object_get_parent (o);
  while (o && o != GST_OBJECT (pipeline)) {
    GstObject *p;
    GST_OBJECT_LOCK (o);
    names = g_list_prepend (names, g_strdup (GST_OBJECT_NAME (o)));
    GST_OBJECT_UNLOCK (o);
    p = gst_object_get_parent (o);
    gst_object_unref (o);
    o = p;
  }
  if (o)
    gst_object_unref (o);

  for (l = names; l; l = l->next) {
    if (s->len)
      g_string_append_c (s, '/');
    g_string_append (s, l->data);
  }
  g_list_free_full (names, g_free);
  return g_string_free (s, FALSE);
}

static GstElement *
top_pipeline (GstObject * o)
{
  GstObject *top = NULL;
  o = gst_object_ref (o);
  while (o) {
    GstObject *p = gst_object_get_parent (o);
    if (GST_IS_PIPELINE (o)) {
      if (top)
        gst_object_unref (top);
      top = gst_object_ref (o);
    }
    gst_object_unref (o);
    o = p;
  }
  return top ? GST_ELEMENT (top) : NULL;
}

static gchar *
pipeline_uid (GstElement * pipeline)
{
  return g_strdup_printf ("%p", (void *) pipeline);
}

/* ------------------------------------------------------------------------- */
/* hooks                                                                     */

static inline void
record_output (GstElement * el, guint64 ts)
{
  /* el pushes a buffer on one of its src pads: match with oldest arrival */
  ElemStats *es = get_elem_stats (el);
  if (es->len) {
    guint64 arr = es->ring[es->head];
    guint64 d = ts > arr ? ts - arr : 0;
    es->head = (es->head + 1) % FIFO_MAX;
    es->len--;
    es->proc_sum += d;
    es->proc_cnt++;
    if (d > es->proc_max)
      es->proc_max = d;
  }
}

static void
handle_push_pre (GstScopeTracer * self, guint64 ts, GstPad * pad,
    guint nbufs, guint64 nbytes, GstClockTime pts)
{
  GstObject *parent;
  PadStats *ps;
  GstElement *to_unref = NULL;
  gint gen;
  gboolean stale;

  if (is_internal_proxy (pad) || GST_IS_GHOST_PAD (pad))
    return;
  parent = GST_OBJECT_PARENT (pad);
  if (!parent || !GST_IS_ELEMENT (parent))
    return;

  gen = g_atomic_int_get (&self->topo_gen);

  g_mutex_lock (&self->lock);
  ps = get_pad_stats (pad);
  stale = ps->cache_gen != gen;
  if (G_UNLIKELY (stale)) {
    /* resolve the downstream leaf element without holding our lock */
    GstElement *d = NULL;
    GstPad *peer;
    g_mutex_unlock (&self->lock);
    peer = resolve_leaf_pad (gst_pad_get_peer (pad));
    if (peer) {
      d = gst_pad_get_parent_element (peer);
      gst_object_unref (peer);
    }
    g_mutex_lock (&self->lock);
    to_unref = ps->downstream;
    ps->downstream = d;
    ps->downstream_is_sink = d &&
        GST_OBJECT_FLAG_IS_SET (d, GST_ELEMENT_FLAG_SINK);
    ps->cache_gen = gen;
  }

  ps->buffers += nbufs;
  ps->bytes += nbytes;
  if (GST_CLOCK_TIME_IS_VALID (pts))
    ps->last_pts = pts;
  ps->push_start = ts;

  record_output (GST_ELEMENT (parent), ts);

  if (ps->downstream && !ps->downstream_is_sink) {
    ElemStats *ds = get_elem_stats (ps->downstream);
    if (ds->len == FIFO_MAX) {          /* drop oldest */
      ds->head = (ds->head + 1) % FIFO_MAX;
      ds->len--;
      ds->fifo_overflows++;
    }
    ds->ring[(ds->head + ds->len) % FIFO_MAX] = ts;
    ds->len++;
  }
  g_mutex_unlock (&self->lock);

  if (to_unref)
    gst_object_unref (to_unref);
}

static void
handle_push_post (GstScopeTracer * self, guint64 ts, GstPad * pad,
    GstFlowReturn res)
{
  PadStats *ps;

  if (is_internal_proxy (pad) || GST_IS_GHOST_PAD (pad))
    return;
  ps = g_object_get_qdata (G_OBJECT (pad), pad_quark);
  if (!ps)
    return;

  g_mutex_lock (&self->lock);
  if (res != GST_FLOW_OK && res != GST_FLOW_FLUSHING) {
    ps->flow_errors++;
    ps->last_flow = res;
  }
  if (ps->downstream && ps->downstream_is_sink && ps->push_start) {
    ElemStats *ds = get_elem_stats (ps->downstream);
    guint64 d = ts > ps->push_start ? ts - ps->push_start : 0;
    ds->sink_sum += d;
    ds->sink_cnt++;
    if (d > ds->sink_max)
      ds->sink_max = d;
  }
  g_mutex_unlock (&self->lock);
}

static void
do_push_buffer_pre (GstScopeTracer * self, guint64 ts, GstPad * pad,
    GstBuffer * buf)
{
  handle_push_pre (self, ts, pad, 1, gst_buffer_get_size (buf),
      GST_BUFFER_PTS (buf));
}

static gboolean
sum_list (GstBuffer ** buf, guint idx, gpointer user)
{
  *(guint64 *) user += gst_buffer_get_size (*buf);
  return TRUE;
}

static void
do_push_list_pre (GstScopeTracer * self, guint64 ts, GstPad * pad,
    GstBufferList * list)
{
  guint64 bytes = 0;
  guint n = gst_buffer_list_length (list);
  GstClockTime pts = GST_CLOCK_TIME_NONE;
  gst_buffer_list_foreach (list, sum_list, &bytes);
  if (n > 0)
    pts = GST_BUFFER_PTS (gst_buffer_list_get (list, n - 1));
  handle_push_pre (self, ts, pad, n, bytes, pts);
}

static void
do_push_post (GstScopeTracer * self, guint64 ts, GstPad * pad,
    GstFlowReturn res)
{
  handle_push_post (self, ts, pad, res);
}

static void
bump_topology (GstScopeTracer * self)
{
  g_atomic_int_inc (&self->topo_gen);
}

static void
do_pad_link_post (GstScopeTracer * self, guint64 ts, GstPad * src,
    GstPad * sink, GstPadLinkReturn res)
{
  bump_topology (self);
}

static void
do_pad_unlink_post (GstScopeTracer * self, guint64 ts, GstPad * src,
    GstPad * sink, gboolean res)
{
  bump_topology (self);
}

static void
do_bin_change_post (GstScopeTracer * self, guint64 ts, GstBin * bin,
    GstElement * el, gboolean res)
{
  bump_topology (self);
}

static void
add_pipeline (GstScopeTracer * self, GstElement * el)
{
  guint i;
  g_mutex_lock (&self->lock);
  for (i = 0; i < self->pipelines->len; i++) {
    GstElement *p = g_weak_ref_get (g_ptr_array_index (self->pipelines, i));
    if (p) {
      gboolean same = p == el;
      gst_object_unref (p);
      if (same) {
        g_mutex_unlock (&self->lock);
        return;
      }
    }
  }
  {
    GWeakRef *ref = g_new0 (GWeakRef, 1);
    g_weak_ref_init (ref, el);
    g_ptr_array_add (self->pipelines, ref);
  }
  g_mutex_unlock (&self->lock);
}

static void
do_element_new (GstScopeTracer * self, guint64 ts, GstElement * el)
{
  if (GST_IS_PIPELINE (el))
    add_pipeline (self, el);
  bump_topology (self);
}

static void send_event (GstScopeTracer * self, GstElement * pipeline,
    GString * ev, guint64 ts);

static void
do_element_post_message_pre (GstScopeTracer * self, guint64 ts,
    GstElement * element, GstMessage * msg)
{
  GstMessageType t = GST_MESSAGE_TYPE (msg);
  GstElement *pipeline;
  GString *j;
  gchar *src_id;

  /* bins re-post their children's messages: only look at the original post */
  if (GST_OBJECT (element) != GST_MESSAGE_SRC (msg))
    return;

  /* exact match: extended message types (GST_MESSAGE_EXTENDED + n) share
   * the low bits with EOS / ERROR / WARNING, so a bitmask test is wrong */
  switch (t) {
    case GST_MESSAGE_QOS:
    case GST_MESSAGE_ERROR:
    case GST_MESSAGE_WARNING:
    case GST_MESSAGE_EOS:
    case GST_MESSAGE_STATE_CHANGED:
      break;
    default:
      return;
  }

  if (t == GST_MESSAGE_QOS) {
    GstFormat fmt;
    guint64 processed = 0, dropped = 0;
    GstObject *src = GST_MESSAGE_SRC (msg);
    gst_message_parse_qos_stats (msg, &fmt, &processed, &dropped);
    if (src && GST_IS_ELEMENT (src) && fmt == GST_FORMAT_BUFFERS) {
      g_mutex_lock (&self->lock);
      {
        ElemStats *es = get_elem_stats (GST_ELEMENT (src));
        es->qos_dropped = dropped;
        es->qos_processed = processed;
      }
      g_mutex_unlock (&self->lock);
    }
    return;
  }

  /* only pipeline-level state changes are interesting */
  /* sinks inside bins post EOS too; only the pipeline-level EOS matters */
  if (t == GST_MESSAGE_EOS && !GST_IS_PIPELINE (GST_MESSAGE_SRC (msg)))
    return;

  if (t == GST_MESSAGE_STATE_CHANGED) {
    if (!GST_IS_PIPELINE (GST_MESSAGE_SRC (msg)))
      return;
    /* pipelines created without a factory never hit element-new */
    if (!GST_OBJECT_PARENT (GST_MESSAGE_SRC (msg)))
      add_pipeline (self, GST_ELEMENT (GST_MESSAGE_SRC (msg)));
  }

  if (!GST_MESSAGE_SRC (msg))
    return;
  pipeline = top_pipeline (GST_MESSAGE_SRC (msg));
  if (!pipeline)
    return;

  j = g_string_new (NULL);
  g_string_append_printf (j, "{\"ts\":%" G_GUINT64_FORMAT ",\"level\":", ts);
  src_id = GST_IS_ELEMENT (GST_MESSAGE_SRC (msg)) &&
      GST_MESSAGE_SRC (msg) != GST_OBJECT (pipeline) ?
      element_id (GST_ELEMENT (GST_MESSAGE_SRC (msg)), pipeline) :
      g_strdup (GST_OBJECT_NAME (GST_MESSAGE_SRC (msg)));

  if (t == GST_MESSAGE_ERROR || t == GST_MESSAGE_WARNING) {
    GError *err = NULL;
    gchar *dbg = NULL;
    if (t == GST_MESSAGE_ERROR)
      gst_message_parse_error (msg, &err, &dbg);
    else
      gst_message_parse_warning (msg, &err, &dbg);
    json_str (j, t == GST_MESSAGE_ERROR ? "error" : "warning");
    g_string_append (j, ",\"src\":");
    json_str (j, src_id);
    g_string_append (j, ",\"msg\":");
    json_str (j, err ? err->message : "");
    g_string_append (j, ",\"debug\":");
    json_str (j, dbg);
    g_clear_error (&err);
    g_free (dbg);
  } else if (t == GST_MESSAGE_EOS) {
    json_str (j, "eos");
    g_string_append (j, ",\"src\":");
    json_str (j, src_id);
    g_string_append (j, ",\"msg\":\"EOS\"");
  } else {
    GstState o, n, p;
    gchar *m;
    gst_message_parse_state_changed (msg, &o, &n, &p);
    m = g_strdup_printf ("%s -> %s", gst_element_state_get_name (o),
        gst_element_state_get_name (n));
    json_str (j, "state");
    g_string_append (j, ",\"src\":");
    json_str (j, src_id);
    g_string_append (j, ",\"msg\":");
    json_str (j, m);
    g_free (m);
  }
  g_string_append_c (j, '}');
  g_free (src_id);

  send_event (self, pipeline, j, ts);
  g_string_free (j, TRUE);
  gst_object_unref (pipeline);
}

/* ------------------------------------------------------------------------- */
/* reporting                                                                 */

static void
send_json (GstScopeTracer * self, GString * s)
{
  ssize_t r;
  if (self->sock < 0)
    return;
  r = sendto (self->sock, s->str, s->len, MSG_DONTWAIT | MSG_NOSIGNAL,
      (struct sockaddr *) &self->addr, self->addr_len);
  if (r < 0) {
    if (errno == EMSGSIZE && !self->warned_send) {
      GST_WARNING ("gstscope: message too large (%" G_GSIZE_FORMAT " bytes)",
          s->len);
      self->warned_send = TRUE;
    }
    /* ENOENT / ECONNREFUSED / EAGAIN: daemon not running or busy, ignore */
  }
}

typedef struct
{
  GstElement *el;
  gchar *id;
  gboolean is_queue;
  guint q_cur_buf, q_max_buf;
  guint64 q_cur_time, q_max_time;
  GPtrArray *pads;                 /* src pads, refs */
} LeafInfo;

static void
leaf_info_clear (LeafInfo * li)
{
  gst_object_unref (li->el);
  g_free (li->id);
  if (li->pads)
    g_ptr_array_unref (li->pads);
}

static GPtrArray *src_pads (GstElement * el);

static GArray *
collect_leaves (GstElement * pipeline)
{
  GArray *arr = g_array_new (FALSE, TRUE, sizeof (LeafInfo));
  GstIterator *it = gst_bin_iterate_recurse (GST_BIN (pipeline));
  GValue v = G_VALUE_INIT;
  gboolean done = FALSE;

  g_array_set_clear_func (arr, (GDestroyNotify) leaf_info_clear);

  while (!done) {
    switch (gst_iterator_next (it, &v)) {
      case GST_ITERATOR_OK:{
        GstElement *el = g_value_get_object (&v);
        if (!GST_IS_BIN (el)) {
          LeafInfo li = { 0, };
          GstElementFactory *f = gst_element_get_factory (el);
          const gchar *fname = f ? GST_OBJECT_NAME (f) : "";
          li.el = gst_object_ref (el);
          li.id = element_id (el, pipeline);
          li.is_queue = g_str_equal (fname, "queue") ||
              g_str_equal (fname, "queue2");
          li.pads = src_pads (el);
          g_array_append_val (arr, li);
        }
        g_value_reset (&v);
        break;
      }
      case GST_ITERATOR_RESYNC:
        g_array_set_size (arr, 0);
        gst_iterator_resync (it);
        break;
      default:
        done = TRUE;
        break;
    }
  }
  g_value_unset (&v);
  gst_iterator_free (it);
  return arr;
}

static GPtrArray *
src_pads (GstElement * el)
{
  GPtrArray *pads = g_ptr_array_new_with_free_func (gst_object_unref);
  GList *l;
  GST_OBJECT_LOCK (el);
  for (l = el->srcpads; l; l = l->next)
    g_ptr_array_add (pads, gst_object_ref (l->data));
  GST_OBJECT_UNLOCK (el);
  return pads;
}

static const gchar *
find_leaf_id (GArray * leaves, GstElement * el)
{
  guint i;
  for (i = 0; i < leaves->len; i++) {
    LeafInfo *li = &g_array_index (leaves, LeafInfo, i);
    if (li->el == el)
      return li->id;
  }
  return NULL;
}

static void
header (GstScopeTracer * self, GString * s, const gchar * type,
    GstElement * pipeline, guint64 now)
{
  gchar *uid = pipeline_uid (pipeline);
  g_string_append_printf (s, "{\"v\":1,\"type\":\"%s\",\"pid\":%d,\"proc\":",
      type, self->pid);
  json_str (s, self->proc_name);
  g_string_append (s, ",\"pipeline\":");
  json_str (s, GST_OBJECT_NAME (pipeline));
  g_string_append (s, ",\"uid\":");
  json_str (s, uid);
  g_string_append_printf (s, ",\"ts\":%" G_GUINT64_FORMAT, now);
  g_free (uid);
}

/* events are sent right away (not with the 1 s report) so that errors of
 * pipelines that fail and exit immediately are not lost */
static void
send_event (GstScopeTracer * self, GstElement * pipeline, GString * ev,
    guint64 ts)
{
  GString *s = g_string_new (NULL);
  header (self, s, "event", pipeline, ts);
  g_string_append (s, ",\"events\":[");
  g_string_append_len (s, ev->str, ev->len);
  g_string_append (s, "]}");
  send_json (self, s);
  g_string_free (s, TRUE);
}

static void
report_topology (GstScopeTracer * self, GstElement * pipeline, GArray * leaves,
    guint64 now)
{
  GString *s = g_string_new (NULL);
  guint i, j;
  gboolean first;
  gchar *ver = gst_version_string ();

  header (self, s, "topology", pipeline, now);
  g_string_append (s, ",\"gst\":");
  json_str (s, ver);
  g_string_append (s, ",\"tracer\":\"" GSTSCOPE_VERSION "\"");
  g_string_append_printf (s, ",\"state\":\"%s\"",
      gst_element_state_get_name (GST_STATE (pipeline)));
  g_free (ver);

  g_string_append (s, ",\"elements\":[");
  for (i = 0; i < leaves->len; i++) {
    LeafInfo *li = &g_array_index (leaves, LeafInfo, i);
    GstElementFactory *f = gst_element_get_factory (li->el);
    const gchar *klass = f ? gst_element_factory_get_metadata (f,
        GST_ELEMENT_METADATA_KLASS) : "";
    if (i)
      g_string_append_c (s, ',');
    g_string_append (s, "{\"id\":");
    json_str (s, li->id);
    g_string_append (s, ",\"factory\":");
    json_str (s, f ? GST_OBJECT_NAME (f) : G_OBJECT_TYPE_NAME (li->el));
    g_string_append (s, ",\"klass\":");
    json_str (s, klass);
    g_string_append_printf (s, ",\"state\":\"%s\",\"sink\":%s,\"source\":%s}",
        gst_element_state_get_name (GST_STATE (li->el)),
        GST_OBJECT_FLAG_IS_SET (li->el, GST_ELEMENT_FLAG_SINK) ? "true" : "false",
        GST_OBJECT_FLAG_IS_SET (li->el, GST_ELEMENT_FLAG_SOURCE) ? "true" : "false");
  }
  g_string_append (s, "],\"links\":[");

  first = TRUE;
  for (i = 0; i < leaves->len; i++) {
    LeafInfo *li = &g_array_index (leaves, LeafInfo, i);
    GPtrArray *pads = li->pads;
    for (j = 0; j < pads->len; j++) {
      GstPad *pad = g_ptr_array_index (pads, j);
      GstPad *peer = resolve_leaf_pad (gst_pad_get_peer (pad));
      GstElement *pel;
      const gchar *pid;
      GstCaps *caps;
      gchar *pname, *peername;

      if (!peer)
        continue;
      pel = gst_pad_get_parent_element (peer);
      pid = pel ? find_leaf_id (leaves, pel) : NULL;
      if (!pid) {
        if (pel)
          gst_object_unref (pel);
        gst_object_unref (peer);
        continue;
      }
      if (!first)
        g_string_append_c (s, ',');
      first = FALSE;

      pname = gst_pad_get_name (pad);
      peername = gst_pad_get_name (peer);
      g_string_append (s, "{\"src\":");
      json_str (s, li->id);
      g_string_append (s, ",\"src_pad\":");
      json_str (s, pname);
      g_string_append (s, ",\"sink\":");
      json_str (s, pid);
      g_string_append (s, ",\"sink_pad\":");
      json_str (s, peername);
      g_string_append (s, ",\"caps\":");
      caps = gst_pad_get_current_caps (pad);
      if (caps) {
        gchar *c = gst_caps_to_string (caps);
        if (strlen (c) > CAPS_MAX)
          c[CAPS_MAX] = '\0';
        json_str (s, c);
        g_free (c);
        gst_caps_unref (caps);
      } else {
        g_string_append (s, "null");
      }
      g_string_append_c (s, '}');
      g_free (pname);
      g_free (peername);
      gst_object_unref (pel);
      gst_object_unref (peer);
    }
  }
  g_string_append (s, "]}");
  send_json (self, s);
  g_string_free (s, TRUE);
}

static void
report_stats (GstScopeTracer * self, GstElement * pipeline, GArray * leaves,
    guint64 now, guint64 dt_ns)
{
  GString *s = g_string_new (NULL);
  guint i, j;
  gboolean first;
  double dt = dt_ns > 0 ? dt_ns / 1e9 : 1.0;

  /* queue levels: query without our lock held */
  for (i = 0; i < leaves->len; i++) {
    LeafInfo *li = &g_array_index (leaves, LeafInfo, i);
    if (li->is_queue)
      g_object_get (li->el, "current-level-buffers", &li->q_cur_buf,
          "max-size-buffers", &li->q_max_buf, "current-level-time",
          &li->q_cur_time, "max-size-time", &li->q_max_time, NULL);
  }

  header (self, s, "stats", pipeline, now);
  g_string_append_printf (s, ",\"dt_ms\":%.1f", dt * 1000.0);
  g_string_append_printf (s, ",\"state\":\"%s\"",
      gst_element_state_get_name (GST_STATE (pipeline)));

  g_mutex_lock (&self->lock);

  g_string_append (s, ",\"pads\":[");
  first = TRUE;
  for (i = 0; i < leaves->len; i++) {
    LeafInfo *li = &g_array_index (leaves, LeafInfo, i);
    GPtrArray *pads = li->pads;
    for (j = 0; j < pads->len; j++) {
      GstPad *pad = g_ptr_array_index (pads, j);
      PadStats *ps = g_object_get_qdata (G_OBJECT (pad), pad_quark);
      guint64 db, dbytes;
      if (!ps)
        continue;
      db = ps->buffers - ps->last_buffers;
      dbytes = ps->bytes - ps->last_bytes;
      ps->last_buffers = ps->buffers;
      ps->last_bytes = ps->bytes;
      if (!first)
        g_string_append_c (s, ',');
      first = FALSE;
      g_string_append (s, "{\"el\":");
      json_str (s, li->id);
      g_string_append (s, ",\"pad\":");
      json_str (s, GST_OBJECT_NAME (pad));
      g_string_append_printf (s,
          ",\"fps\":%.2f,\"kbps\":%.1f,\"buffers\":%" G_GUINT64_FORMAT
          ",\"bytes\":%" G_GUINT64_FORMAT ",\"flow_errors\":%" G_GUINT64_FORMAT,
          db / dt, dbytes * 8.0 / 1000.0 / dt, ps->buffers, ps->bytes,
          ps->flow_errors);
      if (ps->flow_errors) {
        g_string_append (s, ",\"last_flow\":");
        json_str (s, gst_flow_get_name (ps->last_flow));
      }
      if (GST_CLOCK_TIME_IS_VALID (ps->last_pts))
        g_string_append_printf (s, ",\"pts_ms\":%.1f", ps->last_pts / 1e6);
      g_string_append_c (s, '}');
    }
  }

  g_string_append (s, "],\"elements\":[");
  first = TRUE;
  for (i = 0; i < leaves->len; i++) {
    LeafInfo *li = &g_array_index (leaves, LeafInfo, i);
    ElemStats *es = g_object_get_qdata (G_OBJECT (li->el), elem_quark);
    if (!es && !li->is_queue)
      continue;
    if (!first)
      g_string_append_c (s, ',');
    first = FALSE;
    g_string_append (s, "{\"id\":");
    json_str (s, li->id);
    if (es) {
      if (es->proc_cnt)
        g_string_append_printf (s,
            ",\"proc_avg_us\":%.1f,\"proc_max_us\":%.1f,\"proc_n\":%"
            G_GUINT64_FORMAT, es->proc_sum / 1e3 / es->proc_cnt,
            es->proc_max / 1e3, es->proc_cnt);
      if (es->sink_cnt)
        g_string_append_printf (s,
            ",\"sink_avg_us\":%.1f,\"sink_max_us\":%.1f,\"sink_n\":%"
            G_GUINT64_FORMAT, es->sink_sum / 1e3 / es->sink_cnt,
            es->sink_max / 1e3, es->sink_cnt);
      g_string_append_printf (s, ",\"pending\":%u", es->len);
      if (es->qos_processed || es->qos_dropped)
        g_string_append_printf (s,
            ",\"qos_dropped\":%" G_GUINT64_FORMAT ",\"qos_processed\":%"
            G_GUINT64_FORMAT, es->qos_dropped, es->qos_processed);
      es->proc_sum = es->proc_cnt = es->proc_max = 0;
      es->sink_sum = es->sink_cnt = es->sink_max = 0;
    }
    if (li->is_queue)
      g_string_append_printf (s,
          ",\"queue\":{\"buffers\":%u,\"max_buffers\":%u,\"time_ms\":%.1f,"
          "\"max_time_ms\":%.1f}", li->q_cur_buf, li->q_max_buf,
          li->q_cur_time / 1e6, li->q_max_time / 1e6);
    g_string_append_c (s, '}');
  }

  g_string_append (s, "]}");
  g_mutex_unlock (&self->lock);

  send_json (self, s);
  g_string_free (s, TRUE);
}

static gpointer
report_thread (gpointer data)
{
  GstScopeTracer *self = data;
  guint64 last = gst_util_get_timestamp ();
  gint last_gen = -1;
  guint tick = 0;

  g_mutex_lock (&self->thread_lock);
  while (!self->stop) {
    gint64 end = g_get_monotonic_time () + self->interval_ms * 1000;
    GPtrArray *alive;
    guint i;
    guint64 now;
    gint gen;
    gboolean send_topo;

    while (!self->stop &&
        g_cond_wait_until (&self->thread_cond, &self->thread_lock, end));
    if (self->stop)
      break;
    g_mutex_unlock (&self->thread_lock);

    now = gst_util_get_timestamp ();
    gen = g_atomic_int_get (&self->topo_gen);
    send_topo = (gen != last_gen) || (tick % TOPO_EVERY == 0);
    last_gen = gen;
    tick++;

    /* snapshot alive pipelines */
    alive = g_ptr_array_new_with_free_func (gst_object_unref);
    g_mutex_lock (&self->lock);
    for (i = 0; i < self->pipelines->len;) {
      GWeakRef *ref = g_ptr_array_index (self->pipelines, i);
      GstElement *p = g_weak_ref_get (ref);
      if (!p) {
        g_weak_ref_clear (ref);
        g_free (ref);
        g_ptr_array_remove_index_fast (self->pipelines, i);
        continue;
      }
      g_ptr_array_add (alive, p);
      i++;
    }
    g_mutex_unlock (&self->lock);

    for (i = 0; i < alive->len; i++) {
      GstElement *p = g_ptr_array_index (alive, i);
      GArray *leaves = collect_leaves (p);
      if (send_topo)
        report_topology (self, p, leaves, now);
      report_stats (self, p, leaves, now, now - last);
      g_array_unref (leaves);
    }
    g_ptr_array_unref (alive);
    last = now;

    g_mutex_lock (&self->thread_lock);
  }
  g_mutex_unlock (&self->thread_lock);
  return NULL;
}

/* ------------------------------------------------------------------------- */
/* GObject                                                                   */

static void
parse_params (GstScopeTracer * self)
{
  gchar *params = NULL;
  const gchar *env;

  /* 1) environment */
  env = g_getenv ("GSTSCOPE_SOCKET");
  if (env && *env) {
    g_free (self->socket_path);
    self->socket_path = g_strdup (env);
  }
  env = g_getenv ("GSTSCOPE_INTERVAL");
  if (env && atoi (env) >= 100)
    self->interval_ms = atoi (env);

  /* 2) GST_TRACERS="gstscope(socket=...,interval=...)" wins */
  g_object_get (self, "params", &params, NULL);
  if (params && *params) {
    gchar *tmp = g_strdup_printf ("p,%s", params);
    GstStructure *st = gst_structure_from_string (tmp, NULL);
    if (st) {
      const gchar *sock = gst_structure_get_string (st, "socket");
      gint iv;
      if (sock) {
        g_free (self->socket_path);
        self->socket_path = g_strdup (sock);
      }
      if (gst_structure_get_int (st, "interval", &iv) && iv >= 100)
        self->interval_ms = iv;
      gst_structure_free (st);
    } else {
      GST_WARNING ("gstscope: cannot parse params '%s'", params);
    }
    g_free (tmp);
  }
  g_free (params);
}

static void
gst_scope_tracer_constructed (GObject * obj)
{
  GstScopeTracer *self = GST_SCOPE_TRACER (obj);
  GstTracer *t = GST_TRACER (obj);
  FILE *f;

  G_OBJECT_CLASS (gst_scope_tracer_parent_class)->constructed (obj);

  parse_params (self);

  self->sock = socket (AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0);
  memset (&self->addr, 0, sizeof (self->addr));
  self->addr.sun_family = AF_UNIX;
  g_strlcpy (self->addr.sun_path, self->socket_path,
      sizeof (self->addr.sun_path));
  self->addr_len = sizeof (self->addr);
  if (self->sock >= 0) {
    int sz = 1 << 20;
    setsockopt (self->sock, SOL_SOCKET, SO_SNDBUF, &sz, sizeof (sz));
  }

  self->pid = getpid ();
  f = fopen ("/proc/self/comm", "r");
  if (f) {
    if (fgets (self->proc_name, sizeof (self->proc_name), f))
      self->proc_name[strcspn (self->proc_name, "\n")] = '\0';
    fclose (f);
  }

  gst_tracing_register_hook (t, "pad-push-pre", G_CALLBACK (do_push_buffer_pre));
  gst_tracing_register_hook (t, "pad-push-post", G_CALLBACK (do_push_post));
  gst_tracing_register_hook (t, "pad-push-list-pre",
      G_CALLBACK (do_push_list_pre));
  gst_tracing_register_hook (t, "pad-push-list-post", G_CALLBACK (do_push_post));
  gst_tracing_register_hook (t, "pad-link-post", G_CALLBACK (do_pad_link_post));
  gst_tracing_register_hook (t, "pad-unlink-post",
      G_CALLBACK (do_pad_unlink_post));
  gst_tracing_register_hook (t, "bin-add-post", G_CALLBACK (do_bin_change_post));
  gst_tracing_register_hook (t, "bin-remove-post",
      G_CALLBACK (do_bin_change_post));
  gst_tracing_register_hook (t, "element-new", G_CALLBACK (do_element_new));
  gst_tracing_register_hook (t, "element-post-message-pre",
      G_CALLBACK (do_element_post_message_pre));

  self->thread = g_thread_new ("gstscope", report_thread, self);
  GST_INFO ("gstscope tracer: socket=%s interval=%u ms", self->socket_path,
      self->interval_ms);
}

static void
gst_scope_tracer_finalize (GObject * obj)
{
  GstScopeTracer *self = GST_SCOPE_TRACER (obj);
  guint i;

  g_mutex_lock (&self->thread_lock);
  self->stop = TRUE;
  g_cond_signal (&self->thread_cond);
  g_mutex_unlock (&self->thread_lock);
  if (self->thread)
    g_thread_join (self->thread);

  for (i = 0; i < self->pipelines->len; i++) {
    GWeakRef *ref = g_ptr_array_index (self->pipelines, i);
    g_weak_ref_clear (ref);
    g_free (ref);
  }
  g_ptr_array_unref (self->pipelines);
  if (self->sock >= 0)
    close (self->sock);
  g_free (self->socket_path);
  g_mutex_clear (&self->lock);
  g_mutex_clear (&self->thread_lock);
  g_cond_clear (&self->thread_cond);

  G_OBJECT_CLASS (gst_scope_tracer_parent_class)->finalize (obj);
}

static void
gst_scope_tracer_class_init (GstScopeTracerClass * klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (klass);
  gobject_class->constructed = gst_scope_tracer_constructed;
  gobject_class->finalize = gst_scope_tracer_finalize;
  pad_quark = g_quark_from_static_string ("gstscope-pad-stats");
  elem_quark = g_quark_from_static_string ("gstscope-elem-stats");
}

static void
gst_scope_tracer_init (GstScopeTracer * self)
{
  self->socket_path = g_strdup (DEFAULT_SOCKET);
  self->interval_ms = DEFAULT_INTERVAL;
  self->sock = -1;
  self->pipelines = g_ptr_array_new ();
  g_mutex_init (&self->lock);
  g_mutex_init (&self->thread_lock);
  g_cond_init (&self->thread_cond);
}

static gboolean
plugin_init (GstPlugin * plugin)
{
  GST_DEBUG_CATEGORY_INIT (gst_scope_debug, "gstscope", 0, "gstscope tracer");
  return gst_tracer_register (plugin, "gstscope", GST_TYPE_SCOPE_TRACER);
}

#ifndef PACKAGE
#define PACKAGE "gstscope"
#endif

GST_PLUGIN_DEFINE (GST_VERSION_MAJOR, GST_VERSION_MINOR, scope,
    "gstscope pipeline profiler tracer", plugin_init, GSTSCOPE_VERSION,
    "MIT/X11", "gstscope", "https://github.com/erdemkkok/f-gstscobe")
