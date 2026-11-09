'use strict';
(() => {
  const $ = id => document.getElementById(id);
  const format = new Intl.NumberFormat('zh-CN', {maximumFractionDigits: 2});
  const date = new Intl.DateTimeFormat('zh-CN', {timeZone: 'Asia/Shanghai', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit'});
  const palette = ['blue', 'purple', 'teal', 'orange'];
  const state = {status: null, data: null, busy: false, pending: false, request: 0, lastQuery: 0, lastStatus: 0, queue: ''};
  function stamp(value) { return value ? date.format(new Date(value)) : '—'; }
  function node(tag, value, kind) { const element = document.createElement(tag); element.textContent = value; if (kind) element.className = kind; return element; }
  async function api(path) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);
    try {
      const response = await fetch(new URL(`./api/${path}`, window.location.href), {credentials: 'same-origin', cache: 'no-store', signal: controller.signal});
      const result = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(response.status === 401 ? '访问未获授权，请检查当前应用的登录状态' : result.message || `历史查询失败（${response.status}）`);
      return result;
    } finally { clearTimeout(timeout); }
  }
  async function checkStatus() {
    state.lastStatus = Date.now();
    try {
      const status = await api('history/status');
      state.status = status;
      $('history-controls').disabled = !status.enabled;
      if (!status.enabled) {
        $('history-health').textContent = '未启用';
        $('history-status').textContent = '当前仅显示实时监控。引入 MySQL 历史扩展并启用持久化后，可查看跨页面刷新保留的趋势与处理指标。';
        return;
      }
      const healthy = status.state === 'RUNNING';
      $('history-health').textContent = healthy ? (status.collector ? '采集正常' : '统计正常') : status.state === 'STARTING' ? '正在连接' : '采集需关注';
      $('history-status').textContent = `${status.sourceId} · 保留最近 ${format.format(status.retentionMillis / 86400000)} 天 · 最近写入 ${stamp(status.lastWriteAt)} · 丢弃统计事件 ${format.format(status.droppedEvents)}${status.skippedQueues ? ` · 未采样队列 ${status.skippedQueues}` : ''}${status.error ? ` · ${status.error}` : ''}`;
      $('history-description').textContent = `MySQL 持久化 · ${status.namespace} · ${format.format(status.sampleIntervalMillis / 1000)} 秒采样 · 历史查询不扫描 Redis`;
      if (!state.data && $('history-queue').value) load();
    } catch (error) {
      $('history-health').textContent = '状态读取失败';
      $('history-status').textContent = error.name === 'AbortError' ? '历史状态读取超时，实时监控保持独立。' : error.message;
    }
  }
  function queues(overview) {
    const select = $('history-queue');
    let added = false;
    // 只保留当前页及选中项，避免页面翻页过程中在浏览器累计无限队列。
    const selected = select.value;
    const names = new Set(overview.queues.map(item => item.name));
    if (selected) names.add(selected);
    const before = new Set([...select.options].map(option => option.value));
    select.replaceChildren(node('option', '请选择队列'));
    select.firstElementChild.value = '';
    for (const name of names) {
      const option = node('option', name); option.value = name; select.append(option);
      added ||= !before.has(name);
    }
    select.value = selected || [...names][0] || '';
    if (state.status?.enabled && (!state.data || (!selected && added))) load();
  }
  function bounds() {
    const range = $('history-range').value;
    if (range !== 'custom') { const to = Date.now(); return {from: to - Number(range), to}; }
    const from = Date.parse($('history-from').value);
    const to = Date.parse($('history-to').value);
    if (!Number.isFinite(from) || !Number.isFinite(to) || from >= to) throw new Error('请选择有效的开始和结束时间。');
    return {from, to};
  }
  async function load() {
    const request = ++state.request;
    if (!state.status?.enabled || !$('history-queue').value) return;
    if (state.busy) { state.pending = true; return; }
    let time;
    try { time = bounds(); }
    catch (error) { $('history-coverage').textContent = error.message; return; }
    const queue = $('history-queue').value;
    state.busy = true;
    $('history-query').disabled = true;
    $('history-coverage').textContent = '正在读取历史分析…';
    try {
      const data = await api(`queues/${encodeURIComponent(queue)}/history?from=${time.from}&to=${time.to}&points=300`);
      if (request !== state.request) return;
      state.data = data; state.queue = queue; state.lastQuery = Date.now();
      render();
    } catch (error) {
      if (request === state.request) {
        const reason = error.name === 'AbortError' ? '历史查询超时，请缩短时间范围。' : error.message;
        $('history-coverage').textContent = `${reason}${state.data ? `；图表仍为上次成功读取的 ${state.queue}。` : ''}`;
        if (!state.data) $('history-empty').textContent = reason;
      }
    } finally {
      state.busy = false; $('history-query').disabled = false;
      if (state.pending) { state.pending = false; load(); }
    }
  }
  function series() {
    const rate = (point, key) => {
      if (!point.counts) return null;
      const span = Math.max(1000, Math.min(state.data.to, point.at + state.data.resolutionMillis) - Math.max(state.data.from, point.at));
      return point.counts[key] * 60000 / span;
    };
    switch ($('history-metric').value) {
      case 'throughput': return {unit: '次/分钟', note: '分钟平均速率 · 重试包含处理权超时恢复', lines: [
        ['成功确认', point => rate(point, 'acknowledged')],
        ['处理异常', point => rate(point, 'handler_failure')],
        ['重试调度', point => rate(point, 'retried')]]};
      case 'latency': return {unit: 'ms', note: '固定直方图合并估算 P95 · 无耗时样本时显示缺口', lines: [
        ['处理 P95', point => point.processing?.count ? point.processing.p95Millis : null],
        ['领取延迟 P95', point => point.claim?.count ? point.claim.p95Millis : null]]};
      default: return {unit: '条', note: '每个展示窗口的数量峰值 · 不同状态峰值未必发生在同一时刻', lines: [
        ['就绪', point => point.peak?.ready ?? null], ['延迟', point => point.peak?.delayed ?? null],
        ['处理中', point => point.peak?.processing ?? null], ['死信', point => point.peak?.dead ?? null]]};
    }
  }
  function render() {
    if (!state.data) return;
    const data = state.data;
    const summary = data.summary;
    for (const [id, key] of [['sent', 'sent'], ['acked', 'acknowledged'], ['failed', 'handler_failure'], ['retried', 'retried']]) {
      $(`history-${id}`).textContent = summary ? format.format(summary.counts[key]) : '—';
    }
    $('history-p95').textContent = summary?.processing.count ? `${format.format(summary.processing.p95Millis)} ms` : '—';
    const chart = series();
    $('history-legend').replaceChildren(...chart.lines.map(([name], index) => {
      const label = node('span', name); label.prepend(node('i', '', `${palette[index]}-bg`)); return label;
    }));
    const values = data.points.flatMap(point => chart.lines.map(([, value]) => value(point)).filter(value => value !== null));
    const max = Math.max(1, ...values);
    const ceiling = max <= 10 ? Math.ceil(max) : Math.ceil(max / 10) * 10;
    const labels = $('history-y').children;
    labels[0].textContent = format.format(ceiling); labels[1].textContent = format.format(ceiling / 2); labels[2].textContent = '0';
    for (let line = 0; line < 4; line++) {
      let drawing = '', connected = false;
      if (chart.lines[line]) for (const point of data.points) {
        const value = chart.lines[line][1](point);
        if (value === null) { connected = false; continue; }
        const x = Math.max(0, Math.min(840, (point.at - data.from) / Math.max(1, data.to - data.from) * 840));
        const y = 158 - value / ceiling * 146;
        drawing += `${connected ? 'L' : 'M'}${x.toFixed(1)},${y.toFixed(1)} l.01,0 `;
        connected = true;
      }
      $(`history-line-${line}`).setAttribute('d', drawing);
    }
    $('history-empty').classList.toggle('hidden', values.length > 0);
    $('history-empty').textContent = '此范围尚无对应数据；采集前或故障期间的缺口不会显示为零。';
    $('history-start').textContent = stamp(data.from); $('history-end').textContent = stamp(data.to);
    $('history-chart-note').textContent = `${chart.note} · ${chart.unit}`;
    const present = data.points.filter(point => point.snapshotPresent || point.metricsPresent).length;
    $('history-coverage').textContent = `${data.queue} · ${present}/${data.points.length} 个窗口有数据 · ${format.format(data.resolutionMillis / 60000)} 分钟/点${summary?.incomplete ? ' · 包含采集缺口或未结束窗口' : ''} · 统计为观测值，不作为业务审计。`;
  }
  $('history-form').addEventListener('submit', event => { event.preventDefault(); load(); });
  $('history-queue').addEventListener('change', load);
  $('history-metric').addEventListener('change', render);
  $('history-range').addEventListener('change', () => {
    const custom = $('history-range').value === 'custom';
    for (const label of document.querySelectorAll('.history-custom')) label.classList.toggle('hidden', !custom);
    if (custom) {
      const local = value => { const d = new Date(value); return new Date(value - d.getTimezoneOffset() * 60000).toISOString().slice(0, 16); };
      if (!$('history-from').value) $('history-from').value = local(Date.now() - 3600000);
      if (!$('history-to').value) $('history-to').value = local(Date.now());
    } else load();
  });
  window.addEventListener('redis-mq:overview', event => queues(event.detail));
  window.setInterval(() => {
    if (document.hidden || !$('drawer').classList.contains('hidden')) return;
    if (Date.now() - state.lastStatus >= 30000) checkStatus();
    if (state.status?.enabled && !state.busy && Date.now() - state.lastQuery >= 60000 && $('history-range').value !== 'custom') load();
  }, 5000);
  checkStatus();
})();
