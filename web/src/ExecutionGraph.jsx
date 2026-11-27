import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  BadgeCheck,
  Bot,
  ChevronDown,
  CircleAlert,
  CircleCheck,
  CircleHelp,
  Clapperboard,
  ClipboardCheck,
  CloudCog,
  Clock3,
  GitBranch,
  Hourglass,
  Layers3,
  ListChecks,
  LoaderCircle,
  MapPin,
  RotateCcw,
  ShieldCheck,
  UserRound,
  WandSparkles,
  Wrench,
  Workflow,
  X,
} from "lucide-react";
import { apiUrl, getExecutionGraph, listJobActivities } from "./api";
import { formatDuration } from "./activity-time";
import {activityCursor,executionActivityDetail,executionActivityHasFullDetail,groupExecutionActivities,mergeExecutionActivities} from "./execution-activities";
import "./execution-activities.css";
import HarnessRunDetails,{formatCompactTokens,tokenUsageTotal} from "./HarnessRunDetails";
import "./role-icons.css";
import {
  defaultCollapsedGroups,
  executionGraphView,
} from "./execution-graph-layout";

const terminal = new Set([
  "COMPLETED",
  "FAILED",
  "CANCELLED",
  "PAUSED",
  "WAITING_USER",
  "SUCCEEDED",
  "BLOCKED",
  "UNKNOWN",
  "TERMINAL",
  "CONTINUED",
  "CONFIRMED",
  "REJECTED",
  "EXPIRED",
  "PUBLISHED",
  "REVIEW_BLOCKED",
  "OPTIONAL_WARNING",
  "REQUIRED_UNAVAILABLE",
  "responded",
  "expired",
]);
const warningStates = new Set([
  "FAILED",
  "UNKNOWN",
  "REVIEW_BLOCKED",
  "OPTIONAL_WARNING",
  "REQUIRED_UNAVAILABLE",
]);
const activityTypes = new Set([
  "reasoning",
  "plan",
  "command",
  "tool",
  "file_change",
  "warning",
  "error",
]);
const stateLabels = {
  CREATED: "已创建",
  QUEUED: "等待调度",
  RUNNING: "执行中",
  WAITING_EXTERNAL: "等待外部结果",
  CANCELLING: "正在取消",
  SUCCEEDED: "已完成",
  FAILED: "失败",
  CANCELLED: "已取消",
  BLOCKED: "等待父任务处理",
  UNKNOWN: "结果待对账",
  COMMITTED: "已提交",
  TERMINAL: "已结束",
  CONTINUED: "已续接",
  AWAITING_CONFIRMATION: "等待确认",
  CONFIRMED: "已确认",
  COMPLETED: "已完成",
  WAITING_USER: "等待用户",
  PENDING: "等待审查",
  PUBLISHED: "审查通过并发布",
  REVIEW_BLOCKED: "审查要求修订",
  OPTIONAL_WARNING: "可选审查不可用",
  REQUIRED_UNAVAILABLE: "必需审查不可用",
  responded: "已回答",
  expired: "已过期",
};
const typeLabels = {
  user_turn: "用户输入",
  continuation: "任务续接",
  primary_harness: "主执行",
  job_group_draft: "任务确认",
  job_group: "并行任务组",
  wait_barrier: "子任务等待",
  job: "执行任务",
  media_batch: "媒体任务组",
  media_draft: "媒体确认",
  media_job: "媒体生成任务",
  review_gate: "审查发布门",
  interaction: "用户决策",
};
const edgeLabels = {
  submitted_by: "派生",
  contains: "包含",
  waits_on: "等待",
  reported_to: "返回",
  continued_by: "续接",
  reviewed_by: "审查",
  review_verdict: "结论",
  requested_interaction: "决策",
};
const errorPhaseLabels = {
  input_validation: "输入校验",
  authorization: "授权",
  capacity: "资源调度",
  submission: "任务提交",
  status: "状态查询",
  execution: "执行",
  collection: "成果收集",
  validation: "成果校验",
  review: "质量审查",
  cancellation: "取消",
  reconciliation: "结果对账",
  cleanup: "资源清理",
  internal: "平台内部",
};
const sideEffectLabels = {
  none: "未发生外部副作用",
  possible: "可能发生外部副作用",
  accepted: "外部任务已接受",
  completed: "外部执行已完成",
  unknown: "外部副作用待确认",
};
const roleIcons={user:UserRound,continuation:RotateCcw,primary_harness:WandSparkles,
 job_confirmation:ClipboardCheck,job_group:GitBranch,wait_barrier:Hourglass,
 reviewer:ListChecks,child_harness:Bot,external_capability:CloudCog,worker:Wrench,
 media_group:Layers3,media_confirmation:BadgeCheck,media_generation:Clapperboard,
 review_gate:ShieldCheck,user_decision:CircleHelp};
const roleFamilies={user:'user',continuation:'user',primary_harness:'harness',child_harness:'harness',
 job_confirmation:'coordination',job_group:'coordination',wait_barrier:'coordination',
 reviewer:'review',review_gate:'review',external_capability:'external',media_generation:'external',
 worker:'execution',media_group:'execution',media_confirmation:'execution',user_decision:'user'};

function elapsed(node, now) {
  if (node.ended_at) return node.duration_ms || 0;
  return Math.max(0, now - new Date(node.started_at).getTime());
}
function edgePath(source, target) {
  if (source.band !== target.band) {
    const sx = source.x + source.width / 2,
      sy = source.y + source.height,
      tx = target.x + target.width / 2,
      ty = target.y,
      middle = (sy + ty) / 2;
    return `M ${sx} ${sy} C ${sx} ${middle}, ${tx} ${middle}, ${tx} ${ty}`;
  }
  const forward = target.x >= source.x,
    sx = forward ? source.x + source.width : source.x,
    tx = forward ? target.x : target.x + target.width,
    sy = source.y + source.height / 2,
    ty = target.y + target.height / 2,
    middle = (sx + tx) / 2;
  return `M ${sx} ${sy} C ${middle} ${sy}, ${middle} ${ty}, ${tx} ${ty}`;
}
function readPositions(key) {
  try {
    return JSON.parse(sessionStorage.getItem(key) || "{}");
  } catch {
    return {};
  }
}
function displayActivityTitle(value){const name=String(value.summary||value.payload?.name||value.kind||'执行步骤'),labels={commandexecution:'执行命令',bash:'执行命令',read:'读取文件',write:'写入文件',edit:'修改文件',filechange:'修改文件',imageview:'查看图片',mcptoolcall:'调用外部工具',dynamictoolcall:'调用工具',plan:'更新执行计划'};return labels[name.toLowerCase()]||name}
function ActivityDetail({activity}){const value=executionActivityDetail(activity,{limit:140});
 return executionActivityHasFullDetail(value)?<details className="execution-activity-detail">
  <summary>{value.preview}</summary><pre>{value.full}</pre></details>:<small title={value.full||value.preview}>{value.preview}</small>}
function activityGroupDetail(value){const latest=[...value.activities].reverse().map(activity=>executionActivityDetail(activity,{limit:100}))
 .find(detail=>detail.kind!=='status'&&!/^\d+ 项文件操作$/.test(detail.preview));
 return [latest?.preview,`${value.operation_count} 项操作`,value.status==='in_progress'?'正在执行':'已完成'].filter(Boolean).join(' · ')}
class DetailBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { failed: false };
  }
  static getDerivedStateFromError() {
    return { failed: true };
  }
  componentDidUpdate(previous) {
    if (previous.identity !== this.props.identity && this.state.failed)
      this.setState({ failed: false });
  }
  render() {
    return this.state.failed ? (
      <div className="execution-details-empty">节点详情暂时无法显示</div>
    ) : (
      this.props.children
    );
  }
}

export default function ExecutionGraph({ thread, items = [] }) {
  const [graph, setGraph] = useState({
    nodes: [],
    edges: [],
    cursor: "",
    metadata: {},
  });
  const [selectedId, setSelectedId] = useState(""),
    [activities, setActivities] = useState([]),
    [history, setHistory] = useState(false);
  const [olderBefore, setOlderBefore] = useState(""),
    [loadingOlder, setLoadingOlder] = useState(false),
    [now, setNow] = useState(Date.now());
  const [graphError, setGraphError] = useState(""),
    [activityError, setActivityError] = useState(""),
    [open, setOpen] = useState(false);
  const [turnId, setTurnId] = useState(""),
    [following, setFollowing] = useState(true),
    [collapsed, setCollapsed] = useState(new Set()),
    [manual, setManual] = useState({}),
    [viewportWidth, setViewportWidth] = useState(0);
  const loadedOlder = useRef(false),
    collapseKey = useRef(""),
    viewport = useRef(null),
    pan = useRef(null),
    drag = useRef(null),
    suppressClick = useRef(false);
  const mergeGraph = (current, value) => {
    const nodes = new Map(current.nodes.map((node) => [node.id, node]));
    for (const node of value.nodes || []) nodes.set(node.id, node);
    const edges = new Map(
      current.edges.map((edge) => [
        `${edge.from}:${edge.to}:${edge.kind}`,
        edge,
      ]),
    );
    for (const edge of value.edges || [])
      edges.set(`${edge.from}:${edge.to}:${edge.kind}`, edge);
    return {
      ...value,
      nodes: [...nodes.values()].sort((a, b) =>
        String(a.started_at).localeCompare(String(b.started_at)),
      ),
      edges: [...edges.values()],
    };
  };

  useEffect(() => {
    setGraph({ nodes: [], edges: [], cursor: "", metadata: {} });
    setSelectedId("");
    setActivities([]);
    setHistory(false);
    setOlderBefore("");
    setOpen(false);
    setTurnId("");
    setFollowing(true);
    setCollapsed(new Set());
    setManual({});
    collapseKey.current = "";
    loadedOlder.current = false;
  }, [thread?.id]);
  useEffect(() => {
    if (!thread) return;
    let stopped = false,
      timer;
    const load = async () => {
      try {
        const value = await getExecutionGraph(thread.id, graph.cursor, history);
        if (!stopped) {
          setGraphError("");
          if (value.reset) {
            setGraph((current) =>
              history ? mergeGraph(current, value) : value,
            );
            if (history && !loadedOlder.current)
              setOlderBefore(value.metadata?.next_history_before || "");
          }
        }
      } catch (value) {
        if (!stopped)
          setGraphError(value.message || "执行图刷新失败，正在重试");
      } finally {
        if (!stopped) timer = setTimeout(load, 1000);
      }
    };
    load();
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
  }, [thread?.id, history, graph.cursor]);
  const loadOlder = async () => {
    if (!thread || !olderBefore || loadingOlder) return;
    setLoadingOlder(true);
    setGraphError("");
    try {
      const value = await getExecutionGraph(thread.id, "", true, olderBefore);
      loadedOlder.current = true;
      setGraph((current) => mergeGraph(current, value));
      setOlderBefore(value.metadata?.next_history_before || "");
    } catch (value) {
      setGraphError(value.message || "更早执行历史加载失败");
    } finally {
      setLoadingOlder(false);
    }
  };
  const currentView = useMemo(
    () =>
      executionGraphView(graph, {
        turnId: "",
        collapsedGroups: collapsed,
        maxWidth: viewportWidth || Infinity,
      }),
    [graph, collapsed, viewportWidth],
  );
  const baseView = useMemo(
    () =>
      executionGraphView(graph, {
        turnId: following ? "" : turnId,
        collapsedGroups: collapsed,
        maxWidth: viewportWidth || Infinity,
      }),
    [graph, turnId, following, collapsed, viewportWidth],
  );
  const selectedTurnId = baseView.selectedTurnId,
    positionKey = `execution-graph:${thread?.id || ""}:${selectedTurnId}`;
  useEffect(() => {
    if (following && selectedTurnId && turnId !== selectedTurnId)
      setTurnId(selectedTurnId);
  }, [following, selectedTurnId, turnId]);
  useEffect(() => {
    if (!selectedTurnId) return;
    const key = `${thread?.id}:${selectedTurnId}`;
    if (collapseKey.current !== key) {
      collapseKey.current = key;
      setCollapsed(defaultCollapsedGroups(graph, selectedTurnId));
      setManual(
        readPositions(`execution-graph:${thread?.id}:${selectedTurnId}`),
      );
      setSelectedId("");
    }
  }, [thread?.id, selectedTurnId, graph.nodes.length]);
  const nodes = useMemo(
    () =>
      baseView.nodes.map((node) =>
        manual[node.id] ? { ...node, ...manual[node.id] } : node,
      ),
    [baseView.nodes, manual],
  );
  const byId = useMemo(
      () => new Map(nodes.map((node) => [node.id, node])),
      [nodes],
    ),
    selected = graph.nodes.find((node) => node.id === selectedId) || null;
  useEffect(() => {
    if (selectedId && !selected) setSelectedId("");
  }, [graph.nodes, selectedId, selected]);
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);
  useEffect(() => {
    if (!open) return;
    const close = (event) => {
      if (event.key === "Escape") setOpen(false);
    };
    addEventListener("keydown", close);
    return () => removeEventListener("keydown", close);
  }, [open]);
  useEffect(() => {
    if (!open || !viewport.current) return;
    const target = viewport.current,
      observer = new ResizeObserver(() =>
        setViewportWidth(Math.max(260, target.clientWidth)),
      );
    observer.observe(target);
    setViewportWidth(Math.max(260, target.clientWidth));
    return () => observer.disconnect();
  }, [open]);
  useEffect(() => {
    setActivities([]);
    setActivityError("");
  }, [selected?.job_id, selected?.retries]);
  useEffect(() => {
    if (!selected?.job_id) return;
    let stopped = false,
      timer,
      cursor = 0;
    const load = async () => {
      try {
        const rows = await listJobActivities(selected.job_id, cursor);
        if (!stopped) setActivityError("");
        if (rows.length) {
          cursor=activityCursor(rows,cursor);
          if(!stopped)setActivities(current=>mergeExecutionActivities(current,rows));
        }
      } catch (value) {
        if (!stopped)
          setActivityError(value.message || "活动记录刷新失败，正在重试");
      } finally {
        if (!stopped && !terminal.has(selected.state))
          timer = setTimeout(load, 800);
      }
    };
    load();
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
  }, [selected?.job_id, selected?.state, selected?.retries]);
  if (!thread) return null;

  const activeCount = graph.nodes.filter(
    (node) => !terminal.has(node.state),
  ).length;
  const primaryActivities = selected?.turn_id
      ? items.filter(
          (item) =>
            item.turn_id === selected.turn_id && activityTypes.has(item.type),
        )
      : [],
    details = selected?.job_id ? groupExecutionActivities(activities) : primaryActivities;
  const harnessActivities=selected?.job_id?activities:primaryActivities;
  const harnessProgress=[...graph.nodes].reverse().find(node=>node.type==='wait_barrier'
    &&node.turn_id===selected?.turn_id)?.progress||null;
  const saveManual = (value) => {
    setManual(value);
    try {
      sessionStorage.setItem(positionKey, JSON.stringify(value));
    } catch {}
  };
  const panStart = (event) => {
    if (event.target.closest?.(".execution-node")) return;
    pan.current = {
      x: event.clientX,
      y: event.clientY,
      left: viewport.current.scrollLeft,
      top: viewport.current.scrollTop,
    };
    viewport.current.setPointerCapture(event.pointerId);
  };
  const panMove = (event) => {
    if (!pan.current) return;
    viewport.current.scrollLeft =
      pan.current.left - (event.clientX - pan.current.x);
    viewport.current.scrollTop =
      pan.current.top - (event.clientY - pan.current.y);
  };
  const panEnd = () => {
    pan.current = null;
  };
  const dragStart = (event, node) => {
    if (event.button !== 0 || event.target.closest?.("button")) return;
    event.stopPropagation();
    const value = {
      id: node.id,
      x: event.clientX,
      y: event.clientY,
      left: node.x,
      top: node.y,
      moved: false,
    };
    drag.current = value;
    const move = (next) => {
      const dx = next.clientX - value.x,
        dy = next.clientY - value.y;
      if (Math.abs(dx) + Math.abs(dy) > 4) value.moved = true;
      value.final = {
        x: Math.max(0, value.left + dx),
        y: Math.max(0, value.top + dy),
      };
      setManual((current) => ({ ...current, [value.id]: value.final }));
    };
    const finish = () => {
      removeEventListener("mousemove", move);
      removeEventListener("mouseup", finish);
      drag.current = null;
      suppressClick.current = value.moved;
      saveManual({
        ...manual,
        [node.id]: value.final || manual[node.id] || { x: node.x, y: node.y },
      });
    };
    addEventListener("mousemove", move);
    addEventListener("mouseup", finish);
  };
  const toggleGroup = (event, node) => {
    event.stopPropagation();
    setCollapsed((current) => {
      const next = new Set(current);
      if (next.has(node.id)) next.delete(node.id);
      else next.add(node.id);
      return next;
    });
  };

  const detailPanel = selected ? (
    <>
      <header>
        <div>
          <b>{selected.role}</b>
          <small>
            {selected.title} · {selected.location_label}
          </small>
        </div>
        <button
          type="button"
          onClick={() => setSelectedId("")}
          aria-label="关闭执行详情"
        >
          <X size={14} />
        </button>
      </header>
      {selected.harness_view?<HarnessRunDetails node={selected} activities={harnessActivities}
        progress={harnessProgress} now={now} activityError={activityError}/>:<div>
        <dl className="execution-node-facts">
          <dt>类型</dt>
          <dd>{typeLabels[selected.type] || selected.type}</dd>
          <dt>状态</dt>
          <dd>{stateLabels[selected.state] || selected.state}</dd>
          <dt>执行位置</dt>
          <dd>{selected.location_label}</dd>
          {typeof selected.progress === "number" && (
            <>
              <dt>进度</dt>
              <dd>
                {selected.progress}% · {selected.phase || "处理中"}
              </dd>
            </>
          )}
          {selected.progress && typeof selected.progress === "object" && (
            <>
              <dt>返回进度</dt>
              <dd>
                {selected.progress.terminal}/{selected.progress.total}
              </dd>
            </>
          )}
          <dt>开始时间</dt>
          <dd>
            {selected.started_at
              ? new Date(selected.started_at).toLocaleString()
              : "—"}
          </dd>
          <dt>结束时间</dt>
          <dd>
            {selected.ended_at
              ? new Date(selected.ended_at).toLocaleString()
              : "—"}
          </dd>
          <dt>耗时</dt>
          <dd>{formatDuration(elapsed(selected, now))}</dd>
          <dt>重试</dt>
          <dd>{selected.retries || 0}</dd>
          {selected.error_code && (
            <>
              <dt>错误代码</dt>
              <dd>{selected.error_code}</dd>
              <dt>失败阶段</dt>
              <dd>{errorPhaseLabels[selected.failed_phase] || selected.failed_phase || "—"}</dd>
              <dt>重试策略</dt>
              <dd>{selected.retryable ? "可按系统策略恢复" : "不可直接重试"}</dd>
              <dt>外部副作用</dt>
              <dd>{sideEffectLabels[selected.side_effect_state] || selected.side_effect_state || "—"}</dd>
            </>
          )}
        </dl>
        {selected.remaining_jobs?.length > 0 && (
          <section className="execution-remaining-jobs">
            <h3>仍在执行</h3>
            {selected.remaining_jobs.map((job) => (
              <div key={job.job_id}>
                <b>{job.local_job_id}</b>
                <small>{job.descriptor_id} · {stateLabels[job.state] || job.state}</small>
                <time>{formatDuration(job.started_at ? Math.max(0, now-Date.parse(job.started_at)) : 0)}</time>
              </div>
            ))}
          </section>
        )}
        {selected.error_summary && (
          <p className="execution-node-error" role="alert">
            <b>{selected.error_summary}</b>
            {selected.error_guidance && <small>{selected.error_guidance}</small>}
          </p>
        )}
        {activityError && (
          <p className="interaction-error" role="alert">
            {activityError}
          </p>
        )}
        {details.length ? (
          details.slice(-30).map((value) => value.kind==='activity_group' ? (
            <details className="execution-activity-group" key={value.id}>
              <summary>
                <span className={value.status==='in_progress'&&!terminal.has(selected.state)?'activity-pulse':''}/>
                <div><b>{value.summary}</b><small>{activityGroupDetail(value)}</small></div>
                <time>{value.raw_count} 条事件</time>
              </summary>
              <div className="execution-activity-children">{value.activities.map(activity=><div key={activity.id||activity.sequence}>
                <b>{displayActivityTitle(activity)}</b><ActivityDetail activity={activity}/>
              </div>)}</div>
            </details>
          ) : (
            <article key={value.id || value.sequence}>
              <span
                className={
                  value.status === "in_progress" &&
                  !terminal.has(selected.state)
                    ? "activity-pulse"
                    : ""
                }
              />
              <div>
                <b>{displayActivityTitle(value)}</b>
                <ActivityDetail activity={value}/>
              </div>
              <time>{value.raw_count>1?`${value.raw_count} 条事件`:value.sequence?`#${value.sequence}`:""}</time>
            </article>
          ))
        ) : (
          <p>暂无活动记录</p>
        )}
        {selected.artifacts?.length > 0 && (
          <section className="execution-files">
            {selected.artifacts.map((file) => (
              <a
                key={file.artifact_id}
                href={apiUrl(`artifacts/${file.artifact_id}`)}
                target="_blank"
                rel="noreferrer"
              >
                <b>{file.filename}</b>
                <small>
                  {file.artifact_role} · {file.output_disposition} · {file.mime}
                </small>
              </a>
            ))}
          </section>
        )}
      </div>}
    </>
  ) : (
    <div className="execution-details-empty">未选择节点</div>
  );
  return (
    <>
      <button
        type="button"
        className={`execution-graph-trigger ${activeCount ? "active" : ""}`}
        title="打开运行图"
        aria-label="打开运行图"
        onClick={() => setOpen(true)}
      >
        <Workflow size={18} />
        {activeCount > 0 && <span>{activeCount}</span>}
      </button>
      {open && (
        <div
          className="execution-graph-backdrop"
          role="presentation"
          onMouseDown={(event) => {
            if (event.target === event.currentTarget) setOpen(false);
          }}
        >
          <section
            className="execution-graph-modal"
            role="dialog"
            aria-modal="true"
            aria-label="运行图"
          >
            <header>
              <div>
                <small>LIVE EXECUTION GRAPH</small>
                <h2>运行图</h2>
              </div>
              <div className="execution-graph-legend">
                <span>
                  <i className="done" />
                  完成
                </span>
                <span>
                  <i className="running" />
                  执行中
                </span>
                <span>
                  <i className="warning" />
                  受阻
                </span>
              </div>
              <button
                type="button"
                onClick={() => saveManual({})}
                title="恢复自动布局"
                aria-label="恢复自动布局"
              >
                <RotateCcw size={15} />
              </button>
              <button
                type="button"
                onClick={() => setOpen(false)}
                title="关闭运行图"
                aria-label="关闭运行图"
              >
                <X size={17} />
              </button>
            </header>
            {graphError && (
              <div className="execution-graph-error" role="alert">
                {graphError}
              </div>
            )}
            {graph.metadata?.truncated && (
              <div className="execution-graph-error" role="status">
                运行图已按服务端上限截断，活跃任务与依赖链优先保留。
              </div>
            )}
            <div className="execution-graph-toolbar">
              <label>
                <span>轮次</span>
                <select
                  className="execution-turn-select"
                  value={selectedTurnId}
                  onChange={(event) => {
                    const value = event.target.value;
                    setTurnId(value);
                    setFollowing(value === currentView.selectedTurnId);
                  }}
                >
                  {baseView.turns.map((turn) => (
                    <option key={turn.id} value={turn.id}>
                      {turn.label} · {stateLabels[turn.state] || turn.state}
                    </option>
                  ))}
                </select>
              </label>
              {!following && (
                <button
                  type="button"
                  className="execution-follow-current"
                  onClick={() => {
                    setTurnId("");
                    setFollowing(true);
                  }}
                >
                  返回当前执行
                </button>
              )}
              <span>
                {baseView.nodes.length} 个节点 · {baseView.edges.length} 条依赖
              </span>
              <div>
                {graph.historical_available && !history && (
                  <button
                    type="button"
                    onClick={() => {
                      loadedOlder.current = false;
                      setHistory(true);
                    }}
                  >
                    展开历史
                  </button>
                )}
                {history && olderBefore && (
                  <button
                    type="button"
                    disabled={loadingOlder}
                    onClick={loadOlder}
                  >
                    {loadingOlder ? "加载中" : "加载更早"}
                  </button>
                )}
              </div>
            </div>
            {baseView.nodes.length ? (
              <div className="execution-graph-body">
                <div
                  className="execution-graph-viewport"
                  ref={viewport}
                  onPointerDown={panStart}
                  onPointerMove={panMove}
                  onPointerUp={panEnd}
                  onPointerCancel={panEnd}
                >
                  <div
                    className="execution-graph-canvas"
                    style={{ width: baseView.width, height: baseView.height }}
                  >
                    <svg
                      className="execution-edges"
                      width={baseView.width}
                      height={baseView.height}
                    >
                      <defs>
                        <marker
                          id="execution-arrow"
                          viewBox="0 0 10 10"
                          refX="9"
                          refY="5"
                          markerWidth="6"
                          markerHeight="6"
                          orient="auto-start-reverse"
                        >
                          <path d="M 0 0 L 10 5 L 0 10 z" />
                        </marker>
                      </defs>
                      {baseView.edges.map((edge) => {
                        const source = byId.get(edge.from),
                          target = byId.get(edge.to);
                        if (!source || !target) return null;
                        const middleX =
                            (source.x + source.width + target.x) / 2,
                          middleY =
                            (source.y +
                              source.height / 2 +
                              target.y +
                              target.height / 2) /
                            2;
                        return (
                          <g
                            key={edge.id}
                            className={`execution-edge ${edge.kind}`}
                          >
                            <path
                              d={edgePath(source, target)}
                              markerEnd="url(#execution-arrow)"
                            />
                            <text x={middleX} y={middleY - 4}>
                              {edgeLabels[edge.kind] || edge.kind}
                            </text>
                          </g>
                        );
                      })}
                    </svg>
                    {nodes.map((node) => (
                      <div
                        key={node.id}
                        data-node-id={node.id}
                        data-band={node.band}
                        data-visual-role={node.visual_role || "worker"}
                        className={`execution-node ${node.type} ${!terminal.has(node.state) ? "active" : ""} ${warningStates.has(node.state) ? "warning" : ""} ${selectedId === node.id ? "selected" : ""}`}
                        style={{
                          left: node.x,
                          top: node.y,
                          width: node.width,
                          height: node.height,
                        }}
                        role="button"
                        tabIndex={0}
                        onClick={() => {
                          if (suppressClick.current) {
                            suppressClick.current = false;
                            return;
                          }
                          setSelectedId(node.id);
                        }}
                        onKeyDown={(event) => {
                          if (event.key === "Enter" || event.key === " ") {
                            event.preventDefault();
                            setSelectedId(node.id);
                          }
                        }}
                        onMouseDown={(event) => dragStart(event, node)}
                      >
                        <span className="execution-location">
                          <MapPin size={10} />
                          {node.location_label}
                        </span>
                        <span className={`execution-state execution-role-icon role-family-${roleFamilies[node.visual_role]||'execution'}`}>
                          {React.createElement(roleIcons[node.visual_role]||Workflow,{size:15})}
                          <i className="execution-status-icon">{!terminal.has(node.state) ? (
                            <LoaderCircle size={8} />
                          ) : warningStates.has(node.state) ? (
                            <CircleAlert size={8} />
                          ) : (
                            <CircleCheck size={8} />
                          )}</i>
                        </span>
                        <strong>{node.role}</strong>
                        <em>{node.title}</em>
                        <small>
                          {stateLabels[node.state] || node.state} ·{" "}
                          <Clock3 size={10} />
                          {formatDuration(elapsed(node, now))}
                          {node.retries ? ` · 重试 ${node.retries}` : ""}
                          {node.harness_view&&node.model_rounds?.length?` · ${node.model_rounds.length} 轮`:""}
                          {node.harness_view&&tokenUsageTotal(node.token_usage)>0?` · ${formatCompactTokens(tokenUsageTotal(node.token_usage))} tokens${node.token_usage_truncated?' 部分':''}`:""}
                        </small>
                        {node.type === "job_group" && (
                          <button
                            type="button"
                            className="execution-group-toggle"
                            aria-label={
                              (collapsed.has(node.id) ? "展开" : "收起") +
                              ` ${node.collapsedCount || graph.edges.filter((edge) => edge.from === node.id && edge.kind === "contains").length} 个任务`
                            }
                            onClick={(event) => toggleGroup(event, node)}
                          >
                            <ChevronDown size={11} />
                            {collapsed.has(node.id)
                              ? `${node.collapsedCount} 个任务`
                              : "收起"}
                          </button>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
                <aside className="execution-details">
                  <DetailBoundary identity={selectedId}>
                    {detailPanel}
                  </DetailBoundary>
                </aside>
              </div>
            ) : (
              <div className="execution-graph-empty">
                {graphError
                  ? "运行图暂时无法加载"
                  : "当前还没有可展示的执行节点"}
              </div>
            )}
          </section>
        </div>
      )}
    </>
  );
}
