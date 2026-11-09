'use strict';
(() => {
  const $ = id => document.getElementById(id);
  const fields = ['ready', 'delayed', 'processing', 'dead'];
  const format = new Intl.NumberFormat('zh-CN');
  const date = new Intl.DateTimeFormat('zh-CN', {timeZone: 'Asia/Shanghai', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false});
  const time = new Intl.DateTimeFormat('zh-CN', {timeZone: 'Asia/Shanghai', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false});
  const state = {overview: null, queueOffset: 0, selected: null, messageOffset: 0, messageState: 'SCHEDULED', samples: [], loading: false, messageRequest: 0, drawerRequest: 0, returnFocus: null};
  const pageSize = 20;
  function text(tag, value, className) { const node = document.createElement(tag); node.textContent = value; if (className) node.className = className; return node; }
  function empty(body, columns, message) { const row = document.createElement('tr'); const cell = text('td', message, 'table-empty'); cell.colSpan = columns; row.append(cell); body.replaceChildren(row); }
  function stamp(value) { return value ? date.format(new Date(value)) : '—'; }
  function badge(label, kind) { const node = text('span', '', `status ${kind}`); node.append(document.createElement('i'), document.createTextNode(label)); return node; }
  function errorMessage(error) { return error.name === 'AbortError' ? '读取超时，请稍后重试' : error.message || '暂时无法读取数据'; }
  async function api(path) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 12000);
    try {
      const response = await fetch(new URL(`./api/${path}`, window.location.href), {credentials: 'same-origin', cache: 'no-store', signal: controller.signal, headers: {Accept: 'application/json'}});
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(response.status === 401 ? '访问未获授权，请检查当前应用的登录状态' : payload.message || `读取失败（${response.status}）`);
      return payload;
    } finally { clearTimeout(timeout); }
  }
  function setConnection(ok, label) {
    $('connection').className = `connection ${ok ? 'ok' : 'error'}`;
    $('connection').lastElementChild.textContent = label;
  }
  function totals(queues) {
    const result = {ready: 0, delayed: 0, processing: 0, dead: 0, expiredLeases: 0};
    for (const queue of queues) if (queue.stats) for (const key of Object.keys(result)) result[key] += queue.stats[key] || 0;
    return result;
  }
  function renderOverview(data) {
    $('namespace').textContent = data.namespace;
    $('namespace').title = data.namespace;
    $('nav-count').textContent = format.format(data.totalQueues);
    $('queue-count').textContent = format.format(data.totalQueues);
    $('scope').textContent = `当前页 ${data.queues.length} 个队列`;
    $('updated').textContent = `更新于 ${time.format(new Date(data.capturedAt))}`;
    const valid = data.queues.filter(queue => queue.stats);
    const result = totals(valid);
    for (const field of fields) $(`stat-${field}`).textContent = valid.length || !data.queues.length ? format.format(result[field]) : '—';
    const partial = valid.length !== data.queues.length;
    $('alert').classList.toggle('hidden', !partial);
    if (partial) $('alert').textContent = '部分队列暂时无法读取，汇总仅包含读取成功的队列。请稍后刷新。';
    $('expired-count').textContent = partial && !valid.length ? '—' : format.format(result.expiredLeases);
    $('dead-count').textContent = partial && !valid.length ? '—' : format.format(result.dead);
    $('expired-note').textContent = result.expiredLeases ? '等待消费者执行超时恢复' : '当前没有过期处理权';
    $('dead-note').textContent = result.dead ? '存在失败消息，建议查看原因' : '当前没有死信消息';
    for (const [kind, count] of [['expired', result.expiredLeases], ['dead', result.dead]]) {
      $(`${kind}-icon`).classList.toggle('warn', count > 0 || partial);
      $(`${kind}-icon`).textContent = count || partial ? '!' : '✓';
    }
    if (partial) {
      $('expired-note').textContent = '部分队列不可读，检查结果不完整';
      $('dead-note').textContent = '部分队列不可读，检查结果不完整';
      setConnection(false, '部分队列不可读');
    } else {
      state.samples.push({...result, at: Date.now()});
      state.samples = state.samples.filter(sample => sample.at >= Date.now() - 300000).slice(-61);
      renderChart();
      setConnection(true, 'Redis 查询成功');
    }
    renderQueues();
    const last = Math.min(data.totalQueues, state.queueOffset + data.queues.length);
    $('queue-page-label').textContent = data.totalQueues ? `第 ${state.queueOffset + 1}–${last} 个 · 共 ${format.format(data.totalQueues)} 个已登记队列` : '暂无已登记队列 · 发送消息或注册监听器后会自动出现';
    $('queue-prev').disabled = state.queueOffset === 0;
    $('queue-next').disabled = state.queueOffset + pageSize >= data.totalQueues;
  }
  function renderChart() {
    const samples = state.samples;
    $('chart-empty').classList.toggle('hidden', samples.length > 1);
    $('chart-empty').textContent = samples.length === 1 ? '已记录首个采样，等待下一次刷新' : '读取后开始采样';
    const max = Math.max(2, ...samples.flatMap(sample => fields.map(key => sample[key])));
    const ceiling = max <= 10 ? Math.ceil(max / 2) * 2 : Math.ceil(max / 10) * 10;
    const labels = $('chart-y').children;
    labels[0].textContent = format.format(ceiling); labels[1].textContent = format.format(ceiling / 2); labels[2].textContent = '0';
    for (const field of fields) {
      const span = samples.length > 1 ? Math.max(1, samples[samples.length - 1].at - samples[0].at) : 1;
      const points = samples.map(sample => [(sample.at - samples[0].at) / span * 840, 158 - sample[field] / ceiling * 146]);
      const line = points.map(([x,y], i) => `${i ? 'L' : 'M'}${x.toFixed(1)},${y.toFixed(1)}`).join(' ');
      $(`trend-${field}`).setAttribute('d', samples.length > 1 ? line : '');
      if (field === 'ready') $('trend-area').setAttribute('d', samples.length > 1 ? `${line} L840,158 L0,158 Z` : '');
    }
    $('chart-start').textContent = samples.length ? time.format(new Date(samples[0].at)) : '开始采样';
  }
  function renderQueues() {
    const body = $('queue-body');
    const query = $('queue-search').value.trim().toLowerCase();
    const queues = (state.overview?.queues || []).filter(queue => queue.name.toLowerCase().includes(query));
    body.replaceChildren();
    if (!queues.length) { empty(body, 8, query ? '当前页没有匹配的队列' : '暂无队列。发送消息、注册消费者或通过配置预登记队列后，会显示在这里。'); return; }
    for (const queue of queues) {
      const row = document.createElement('tr'); row.classList.toggle('selected', queue.name === state.selected);
      const nameCell = document.createElement('td');
      const link = text('button', queue.name, 'queue-link queue-name'); link.addEventListener('click', () => selectQueue(queue.name)); nameCell.append(link); row.append(nameCell);
      const status = document.createElement('td');
      status.append(!queue.stats ? badge('读取失败', 'failed') : queue.stats.expiredLeases ? badge('待恢复', 'warning') : queue.stats.dead ? badge('需关注', 'warning') : queue.stats.ready ? badge('待领取', 'pending') : queue.stats.processing ? badge('处理中', 'good') : queue.stats.delayed ? badge('等待到期', 'info') : badge('空闲', 'neutral'));
      row.append(status);
      for (const key of [...fields, 'total']) row.append(text('td', queue.stats ? format.format(queue.stats[key]) : '—', key === 'dead' && queue.stats?.dead ? 'number-alert' : ''));
      const action = document.createElement('td'); const button = text('button', '查看 →', 'row-action'); button.setAttribute('aria-label', `查看队列 ${queue.name}`); button.addEventListener('click', () => selectQueue(queue.name)); action.append(button); row.append(action); body.append(row);
    }
  }
  function selectQueue(queue) {
    state.selected = queue; state.messageOffset = 0;
    $('message-heading').textContent = queue;
    $('message-subtitle').textContent = '消息实时状态 · 选择一条记录查看完整正文';
    renderQueues(); loadMessages();
    $('messages').scrollIntoView({behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth', block: 'nearest'});
  }
  async function loadMessages() {
    if (!state.selected) return;
    const request = ++state.messageRequest;
    $('message-prev').disabled = true; $('message-next').disabled = true;
    try {
      const data = await api(`queues/${encodeURIComponent(state.selected)}/messages?state=${state.messageState}&offset=${state.messageOffset}&count=${pageSize}`);
      if (request !== state.messageRequest) return;
      const body = $('message-body'); body.replaceChildren();
      if (!data.length) empty(body, 6, '该状态下暂无消息。已成功确认的消息正文会被清理。');
      for (const message of data) {
        const row = document.createElement('tr');
        const id = text('td', message.id.length > 23 ? `${message.id.slice(0, 20)}…` : message.id, 'message-id'); id.title = message.id; row.append(id);
        const status = document.createElement('td'); status.append(messageBadge(message)); row.append(status);
        row.append(text('td', `${message.deliveryCount} / ${message.maxDeliveries}`));
        const at = text('td', stamp(message.availableAt)); at.title = message.leaseUntil ? `处理权到期：${stamp(message.leaseUntil)}` : ''; row.append(at);
        const reason = text('td', message.lastError || '—', 'error-cell'); reason.title = message.lastError || ''; row.append(reason);
        const action = document.createElement('td'); const button = text('button', '详情 →', 'row-action'); button.setAttribute('aria-label', `消息 ${message.id} 详情`); button.addEventListener('click', () => openDetail(message.id, button)); action.append(button); row.append(action); body.append(row);
      }
      $('message-page-label').textContent = data.length ? `第 ${state.messageOffset + 1}–${state.messageOffset + data.length} 条 · 列表不加载消息正文` : '当前页没有消息';
      $('message-prev').disabled = state.messageOffset === 0;
      $('message-next').disabled = data.length < pageSize;
    } catch (error) { if (request === state.messageRequest) empty($('message-body'), 6, errorMessage(error)); }
  }
  function messageBadge(message) {
    if (message.state === 'DEAD') return badge('死信', 'warning');
    if (message.state === 'PROCESSING') return badge('处理中', 'good');
    if (message.ready === undefined) return badge('待执行', 'pending');
    return message.ready ? badge('就绪', 'pending') : badge('延迟', 'info');
  }
  async function openDetail(id, trigger) {
    state.returnFocus = trigger;
    const request = ++state.drawerRequest;
    $('drawer').classList.remove('hidden'); $('drawer-overlay').classList.remove('hidden');
    $('workspace').inert = true; $('sidebar').inert = true;
    $('drawer-close').focus(); document.body.classList.add('drawer-open');
    $('detail-status').textContent = '正在读取消息…'; $('detail-meta').replaceChildren(); $('detail-error').textContent = '—'; $('detail-payload').textContent = '正在读取…';
    try {
      const message = await api(`queues/${encodeURIComponent(state.selected)}/messages/${encodeURIComponent(id)}`);
      if (request !== state.drawerRequest) return;
      $('detail-status').replaceChildren(messageBadge(message));
      const entries = [['队列名称', state.selected], ['消息编号', message.id], ['投递次数', `${message.deliveryCount} / ${message.maxDeliveries}`], ['发送时间', stamp(message.createdAt)], ['可执行时间', stamp(message.availableAt)], ['处理权到期', stamp(message.leaseUntil)]];
      for (const [label, value] of entries) $('detail-meta').append(text('dt', label), text('dd', value));
      $('detail-error').textContent = message.lastError || '无失败记录';
      let payload = message.payload; try { payload = JSON.stringify(JSON.parse(payload), null, 2); } catch (_) { /* 非 JSON 正文也只作为文本显示。 */ }
      $('detail-payload').textContent = payload;
    } catch (error) { if (request === state.drawerRequest) { $('detail-status').textContent = errorMessage(error); $('detail-payload').textContent = '未读取到消息正文'; } }
  }
  function closeDetail() {
    ++state.drawerRequest;
    $('drawer').classList.add('hidden'); $('drawer-overlay').classList.add('hidden');
    $('workspace').inert = false; $('sidebar').inert = false;
    document.body.classList.remove('drawer-open');
    if (state.returnFocus?.isConnected) state.returnFocus.focus(); else $('refresh').focus();
  }
  async function refresh() {
    if (state.loading) return;
    state.loading = true; $('refresh').disabled = true;
    try {
      const data = await api(`overview?offset=${state.queueOffset}&count=${pageSize}`);
      state.overview = data; renderOverview(data);
      window.dispatchEvent(new CustomEvent("redis-mq:overview", {detail: data}));
      if (state.selected) await loadMessages();
    } catch (error) {
      setConnection(false, '读取失败');
      $('alert').textContent = `${errorMessage(error)}。${state.overview ? '当前保留上一次成功读取的数据。' : '尚未获取到队列数据。'}`;
      $('alert').classList.remove('hidden');
      if (!state.overview) empty($('queue-body'), 8, '暂时无法读取队列目录，请稍后刷新。');
    } finally { state.loading = false; $('refresh').disabled = false; }
  }
  $('refresh').addEventListener('click', refresh);
  $('queue-search').addEventListener('input', renderQueues);
  for (const [id, direction] of [['queue-prev', -1], ['queue-next', 1]]) $(id).addEventListener('click', () => { if (state.loading) return; state.queueOffset = Math.max(0, state.queueOffset + direction * pageSize); state.samples = []; refresh(); });
  for (const [id, direction] of [['message-prev', -1], ['message-next', 1]]) $(id).addEventListener('click', () => { state.messageOffset = Math.max(0, state.messageOffset + direction * pageSize); loadMessages(); });
  for (const button of document.querySelectorAll('[data-state]')) button.addEventListener('click', () => {
    for (const tab of document.querySelectorAll('[data-state]')) tab.setAttribute('aria-selected', String(tab === button));
    state.messageState = button.dataset.state; state.messageOffset = 0;
    $('message-state-note').textContent = state.messageState === 'SCHEDULED' ? '待执行包含尚未到期的延迟消息' : state.messageState === 'PROCESSING' ? '处理权到期后由消费者执行恢复' : '不可投递或被拒绝的消息'; loadMessages();
  });
  $('drawer-close').addEventListener('click', closeDetail); $('drawer-overlay').addEventListener('click', closeDetail);
  document.addEventListener('keydown', event => { if (!$('drawer').classList.contains('hidden')) { if (event.key === 'Escape') closeDetail(); if (event.key === 'Tab') { event.preventDefault(); $('drawer-close').focus(); } } });
  for (const link of document.querySelectorAll('.sidebar .nav')) link.addEventListener('click', () => {
    for (const other of document.querySelectorAll('.sidebar .nav')) other.classList.toggle('active', other === link);
  });
  document.addEventListener('visibilitychange', () => { if (!document.hidden && $('drawer').classList.contains('hidden')) refresh(); });
  window.setInterval(() => { if (!document.hidden && $('drawer').classList.contains('hidden')) refresh(); }, 5000);
  refresh();
})();
