<template>
  <section class="dashboard-shell">
    <header class="hero-card">
      <div>
        <p class="eyebrow">DataFilter Pipeline</p>
        <h1>分析报告看板</h1>
        <p class="hero-copy">选择已完成报告查看结果，任务与缓存状态会在后台持续刷新。</p>
      </div>
      <div class="hero-actions">
        <button class="primary-action" :disabled="loading" @click="start({})">立即拉包分析</button>
        <div class="cache-pill" aria-label="缓存统计">
          <span>Raw <b>{{ cache.raw_count ?? '-' }}</b></span>
          <span>Result <b>{{ cache.result_count ?? '-' }}</b></span>
        </div>
      </div>
    </header>

    <p v-if="error" class="error notice">{{ error }}</p>

    <div
      ref="dashboardGrid"
      class="dashboard-grid"
      :class="{ 'is-resizing': resizing === 'sidebar' }"
      :style="{ '--sidebar-width': `${sidebarWidth}px` }"
    >
      <main class="report-column">
        <section class="mode-switch panel" role="tablist" aria-label="看板模式">
          <button
            type="button"
            role="tab"
            :class="{ active: activeMode === 'single' }"
            :aria-selected="activeMode === 'single'"
            @click="activeMode = 'single'"
          >
            <b>单包分析</b>
            <span>查看一份报告的流水线与结果</span>
          </button>
          <button
            type="button"
            role="tab"
            :class="{ active: activeMode === 'compare' }"
            :aria-selected="activeMode === 'compare'"
            @click="activeMode = 'compare'"
          >
            <b>两包比较</b>
            <span>选择两份报告查看差异</span>
          </button>
        </section>

        <template v-if="activeMode === 'single'">
        <section class="panel report-picker">
          <div class="section-heading">
            <div>
              <p class="eyebrow">Completed reports</p>
              <h2>选择报告</h2>
            </div>
            <span class="muted">{{ results.length }} 份结果</span>
          </div>
          <select v-if="results.length" v-model="currentId" class="report-select" @change="select(currentId, { resetResult: true })">
            <option v-for="item in results" :key="item.run_id" :value="item.run_id">
              {{ reportLabel(item) }}
            </option>
          </select>
          <div v-else class="empty-state">
            <b>暂无已完成报告</b>
            <span>点击“立即拉包分析”创建第一份报告。</span>
          </div>
        </section>

        <section v-if="current.pipeline" class="panel pipeline-card">
          <div class="section-heading">
            <div>
              <p class="eyebrow">Current pipeline</p>
              <h2>{{ current.pipeline.batch_id || current.pipeline.run_id }}</h2>
            </div>
            <span :class="['badge', current.pipeline.status]">{{ current.pipeline.status }}</span>
          </div>

          <button v-if="['failed','blocked'].includes(current.pipeline.status)" class="retry-action" :disabled="loading" @click="retry">
            重试（忽略模拟失败）
          </button>

          <section v-if="result" class="result">
            <div class="section-heading compact">
              <div>
                <p class="eyebrow">Report summary</p>
                <h2>结果摘要</h2>
              </div>
            </div>
            <div class="summary-cards">
              <article v-for="(v,k) in result.summary" :key="k">
                <span>{{ k }}</span>
                <b>{{ format(v) }}</b>
              </article>
            </div>

            <h3>Phrase 频次</h3>
            <div v-if="result.chart_data?.length" class="phrase-chart">
              <div v-for="row in result.chart_data" :key="row.label" class="bar-row">
                <span class="bar-label">{{ row.label }}</span>
                <i :style="{ width: `${barWidth(row.value)}%` }"></i>
                <b>{{ row.value }}</b>
              </div>
            </div>
            <p v-else class="muted">暂无 phrase 频次数据。</p>

            <div class="json-heading">
              <h3>预览 JSON</h3>
              <span class="muted">拖动下方把手调整预览高度</span>
            </div>
            <div
              class="json-preview"
              :class="{ 'is-resizing': resizing === 'json' }"
              :style="{ '--json-height': `${jsonHeight}px` }"
            >
              <button
                type="button"
                class="resize-handle resize-handle-y"
                aria-label="拖动调整 JSON 预览高度"
                @pointerdown="startJsonResize"
              ></button>
              <pre>{{ previewText }}</pre>
            </div>
          </section>
          <div v-else class="empty-state soft">
            <b>正在等待结果</b>
            <span>任务完成后会自动加载分析详情。</span>
          </div>
        </section>

        <section v-else class="panel empty-state tall">
          <b>请选择或创建任务</b>
          <span>有完成报告时，系统会自动选中最新的一份。</span>
        </section>
        </template>

        <section v-else class="panel compare-panel">
          <div class="section-heading">
            <div>
              <p class="eyebrow">Compare reports</p>
              <h2>报告对比</h2>
            </div>
            <span class="muted">右侧减左侧</span>
          </div>

          <div v-if="results.length >= 2" class="compare-controls">
            <label>
              <span>左侧基准</span>
              <select v-model="compareLeftId" class="report-select" @change="onCompareSelectionChange">
                <option value="">选择报告</option>
                <option v-for="item in results" :key="`left-${item.run_id}`" :value="item.run_id">
                  {{ reportLabel(item) }}
                </option>
              </select>
            </label>
            <label>
              <span>右侧对照</span>
              <select v-model="compareRightId" class="report-select" @change="onCompareSelectionChange">
                <option value="">选择报告</option>
                <option v-for="item in results" :key="`right-${item.run_id}`" :value="item.run_id">
                  {{ reportLabel(item) }}
                </option>
              </select>
            </label>
            <button class="primary-action compare-action" :disabled="compareDisabled" @click="runCompare">
              {{ compareLoading ? '对比中…' : '开始对比' }}
            </button>
          </div>
          <div v-else class="empty-state soft">
            <b>至少需要两份报告</b>
            <span>任务完成后可在这里对比差异。</span>
          </div>
          <p v-if="compareError" class="error notice">{{ compareError }}</p>

          <section v-if="compareResult" class="compare-report">
            <div class="compare-title-row">
              <div>
                <span class="muted">{{ compareTitle(compareResult.left) }}</span>
                <b>→</b>
                <span class="muted">{{ compareTitle(compareResult.right) }}</span>
              </div>
              <small>{{ compareResult.metadata?.generated_at || '刚刚生成' }}</small>
            </div>

            <h3>摘要变化</h3>
            <div v-if="summaryDeltaEntries.length" class="summary-cards delta-cards">
              <article v-for="row in summaryDeltaEntries" :key="row.key" :class="deltaTone(row.value)">
                <span>{{ row.key }}</span>
                <b>{{ signed(row.value) }}</b>
              </article>
            </div>
            <p v-else class="muted">暂无摘要差异。</p>

            <div class="compare-bars-grid">
              <section>
                <h3>Phrase 变化</h3>
                <div v-if="phraseDeltaRows.length" class="delta-chart">
                  <div v-for="row in phraseDeltaRows" :key="`phrase-${row.label}`" class="delta-row" :class="deltaTone(row.delta)">
                    <span class="bar-label">{{ row.label }}</span>
                    <div class="delta-track"><i :style="deltaStyle(row.delta)"></i></div>
                    <b>{{ signed(row.delta) }}</b>
                  </div>
                </div>
                <p v-else class="muted">暂无 phrase 差异。</p>
              </section>
              <section>
                <h3>Category 变化</h3>
                <div v-if="categoryDeltaRows.length" class="delta-chart">
                  <div v-for="row in categoryDeltaRows" :key="`category-${row.label}`" class="delta-row" :class="deltaTone(row.delta)">
                    <span class="bar-label">{{ row.label }}</span>
                    <div class="delta-track"><i :style="deltaStyle(row.delta)"></i></div>
                    <b>{{ signed(row.delta) }}</b>
                  </div>
                </div>
                <p v-else class="muted">暂无 category 差异。</p>
              </section>
            </div>

            <h3>记录差异</h3>
            <div class="record-diff-cards">
              <article class="added"><span>新增</span><b>{{ recordDiff.added_count || 0 }}</b></article>
              <article class="removed"><span>移除</span><b>{{ recordDiff.removed_count || 0 }}</b></article>
              <article class="changed"><span>变更</span><b>{{ recordDiff.changed_count || 0 }}</b></article>
            </div>
            <div class="record-preview-grid">
              <section v-for="group in recordPreviewGroups" :key="group.key" class="record-preview">
                <div class="record-preview-heading"><b>{{ group.title }}</b><span class="muted">{{ group.items.length }} 条预览</span></div>
                <pre v-if="group.items.length">{{ JSON.stringify(group.items.slice(0, 3), null, 2) }}</pre>
                <p v-else class="muted">暂无预览。</p>
              </section>
            </div>

            <div class="json-heading">
              <h3>完整差异 JSON</h3>
              <span class="muted">拖动下方把手调整预览高度</span>
            </div>
            <div class="json-preview" :class="{ 'is-resizing': resizing === 'json' }" :style="{ '--json-height': `${jsonHeight}px` }">
              <button type="button" class="resize-handle resize-handle-y" aria-label="拖动调整 JSON 预览高度" @pointerdown="startJsonResize"></button>
              <pre>{{ comparePreviewText }}</pre>
            </div>
          </section>
        </section>
      </main>

      <button
        type="button"
        class="resize-handle resize-handle-x"
        aria-label="拖动调整主内容和状态栏宽度"
        @pointerdown="startSidebarResize"
      ></button>

      <aside class="side-column">
        <section class="panel side-panel">
          <div class="section-heading compact">
            <div>
              <p class="eyebrow">Live jobs</p>
              <h2>最近任务</h2>
            </div>
          </div>
          <button v-for="job in jobs.slice(0, 8)" :key="job.run_id" class="job-row" :class="{ active: currentId===job.run_id }" @click="select(job.run_id)">
            <span>
              <b>{{ job.request?.trigger || 'manual' }}</b>
              <small>{{ shortId(job.run_id) }}</small>
            </span>
            <span :class="['badge', job.status]">{{ job.status }}</span>
          </button>
          <p v-if="!jobs.length" class="muted">暂无任务。</p>
        </section>

        <section v-if="current.steps?.length" class="panel side-panel">
          <div class="section-heading compact">
            <div>
              <p class="eyebrow">Worker pipeline</p>
              <h2>步骤状态</h2>
            </div>
          </div>
          <div v-for="step in current.steps" :key="step.step_id" class="step">
            <div class="step-title"><b>{{ step.step_id }}</b><span :class="['badge', step.status]">{{ step.status }}</span></div>
            <small>{{ step.filter }}@{{ step.filter_version }} · {{ step.attempt }}/{{ step.max_attempts }}</small>
            <p v-if="step.error" class="error">{{ step.error }}</p>
            <p v-if="step.next_retry_at" class="muted">下次重试：{{ step.next_retry_at }}</p>
            <code v-if="step.output_cache_key">{{ step.output_cache_key }}</code>
          </div>
        </section>

        <section class="panel side-panel debug-panel">
          <div>
            <p class="eyebrow">Debug</p>
            <h2>调试</h2>
          </div>
          <button :disabled="loading" class="ghost-action" @click="start({ simulate_fail_stage: 'analyze' })">模拟 analyze 失败</button>
        </section>
      </aside>
    </div>
  </section>
</template>
<script setup>
import { computed, onMounted, onUnmounted, reactive, ref } from 'vue'
import { compareReports, createJob, getCacheStatus, getJob, getResult, listJobs, listResults, retryJob } from '../api/jobs'
const jobs=ref([]), results=ref([]), current=reactive({pipeline:null,steps:[]}), currentId=ref(null), result=ref(null), error=ref(''), cache=reactive({}), loading=ref(false), polling=ref(false)
const activeMode=ref('single')
const compareLeftId=ref(''), compareRightId=ref(''), compareLoading=ref(false), compareError=ref(''), compareResult=ref(null)
const dashboardGrid=ref(null), sidebarWidth=ref(340), jsonHeight=ref(420), resizing=ref(null)
let timer, compareRequestSeq=0, activeJsonPreview=null
const refreshCache=async()=>Object.assign(cache, await getCacheStatus())
const loadJobs=async()=>{ jobs.value=(await listJobs()).jobs }
const loadResults=async()=>{ results.value=(await listResults()).items; applyCompareDefaults() }
function applyCompareDefaults(){
  if (results.value.length < 2) return
  if (!compareLeftId.value) compareLeftId.value = results.value[1]?.run_id || ''
  if (!compareRightId.value) compareRightId.value = results.value[0]?.run_id || ''
  if (compareLeftId.value && compareLeftId.value === compareRightId.value) compareRightId.value = results.value.find(item => item.run_id !== compareLeftId.value)?.run_id || ''
}
async function select(id, options = {}){
  if (!id) return
  const isNewSelection = currentId.value !== id
  currentId.value = id
  if (options.resetResult || isNewSelection) result.value = null

  const next = await getJob(id)
  if (currentId.value !== id) return
  Object.assign(current, next)

  if (next.pipeline.status === 'succeeded') {
    const loaded = await getResult(id).catch(() => null)
    if (currentId.value === id) result.value = loaded
  } else if (options.resetResult || isNewSelection) {
    result.value = null
  }
}
async function tick(){ if(polling.value) return; polling.value=true; try{ await loadJobs(); await loadResults(); await refreshCache(); if(!currentId.value && results.value[0]) await select(results.value[0].run_id, { resetResult: true }); else if(currentId.value){ await select(currentId.value, { resetResult: false }) } } finally { polling.value=false } }
async function start(params){ loading.value=true; error.value=''; try{ const r=await createJob(params); await select(r.pipeline.run_id, { resetResult: true }); await tick() }catch(e){ error.value=e.message } finally { loading.value=false } }
async function retry(){ loading.value=true; try{ await retryJob(currentId.value); await tick() } catch(e){ error.value=e.message } finally { loading.value=false } }
function onCompareSelectionChange(){ compareRequestSeq += 1; compareLoading.value=false; compareError.value=''; compareResult.value=null }
async function runCompare(){
  if (compareDisabled.value) return
  const leftId=compareLeftId.value, rightId=compareRightId.value, seq=++compareRequestSeq
  compareLoading.value=true; compareError.value=''
  try{
    const data=await compareReports(leftId, rightId)
    if (seq !== compareRequestSeq || compareLeftId.value !== leftId || compareRightId.value !== rightId) return
    compareResult.value=data
  }catch(e){
    if (seq !== compareRequestSeq || compareLeftId.value !== leftId || compareRightId.value !== rightId) return
    compareError.value=e.message || '对比失败'
  }finally{
    if (seq === compareRequestSeq && compareLeftId.value === leftId && compareRightId.value === rightId) compareLoading.value=false
  }
}
const format=v=> typeof v==='number' ? v.toFixed(2) : v
const signed=v=>{ const n=Number(v) || 0; return n > 0 ? `+${format(n)}` : format(n) }
const shortId=id=> id ? String(id).slice(0, 8) : '-'
const reportLabel=item=> `${item.batch_id || shortId(item.run_id)} · ${item.trigger || 'manual'} · ${item.finished_at || shortId(item.run_id)}`
const compareTitle=side=> side?.label || side?.run_id || '-'
const maxPhrase=computed(()=>Math.max(1, ...(result.value?.chart_data || []).map(row=>Number(row.value) || 0)))
const barWidth=value=>Math.max(6, Math.round(((Number(value) || 0) / maxPhrase.value) * 100))
const compareDisabled=computed(()=>results.value.length<2 || !compareLeftId.value || !compareRightId.value || compareLeftId.value===compareRightId.value || compareLoading.value)
const summaryDeltaEntries=computed(()=>Object.entries(compareResult.value?.summary_delta || {}).map(([key,value])=>({key,value})))
const phraseDeltaRows=computed(()=>(compareResult.value?.phrase_delta || []).slice(0, 100))
const categoryDeltaRows=computed(()=>(compareResult.value?.category_delta || []).slice(0, 100))
const maxDelta=computed(()=>Math.max(1, ...[...phraseDeltaRows.value, ...categoryDeltaRows.value].map(row=>Math.abs(Number(row.delta) || 0))))
const deltaTone=value=>Number(value)>0 ? 'positive' : Number(value)<0 ? 'negative' : 'zero'
const deltaStyle=value=>({ '--delta-size': `${Math.max(3, Math.round((Math.abs(Number(value) || 0) / maxDelta.value) * 50))}%` })
const recordDiff=computed(()=>compareResult.value?.record_diff || {})
const recordPreviewGroups=computed(()=>[
  { key:'added', title:'新增预览', items:recordDiff.value.added_preview || [] },
  { key:'removed', title:'移除预览', items:recordDiff.value.removed_preview || [] },
  { key:'changed', title:'变更预览', items:recordDiff.value.changed_preview || [] }
])
const previewText=computed(()=>{
  const preview=result.value?.records_preview
  if (typeof preview === 'string') return preview
  return JSON.stringify(preview ?? [], null, 2)
})
const comparePreviewText=computed(()=>JSON.stringify(compareResult.value ?? {}, null, 2))
const clamp=(value,min,max)=>Math.min(max,Math.max(min,value))
function beginResize(type, event){
  event.preventDefault()
  resizing.value=type
  document.body.classList.add('is-dragging-panel')
  window.addEventListener('pointermove', type === 'sidebar' ? onSidebarResize : onJsonResize)
  window.addEventListener('pointerup', stopResize, { once: true })
  window.addEventListener('pointercancel', stopResize, { once: true })
}
function startSidebarResize(event){
  if (window.matchMedia('(max-width: 900px)').matches) return
  beginResize('sidebar', event)
  onSidebarResize(event)
}
function onSidebarResize(event){
  const rect=dashboardGrid.value?.getBoundingClientRect()
  if(!rect) return
  const max=Math.min(560, Math.max(300, rect.width * 0.48))
  sidebarWidth.value=clamp(rect.right - event.clientX - 24, 260, max)
}
function startJsonResize(event){
  activeJsonPreview=event.currentTarget?.closest?.('.json-preview') || null
  beginResize('json', event)
  onJsonResize(event)
}
function onJsonResize(event){
  const preview=activeJsonPreview || event.target?.closest?.('.json-preview') || document.querySelector('.json-preview')
  const rect=preview?.getBoundingClientRect()
  if(!rect) return
  jsonHeight.value=clamp(event.clientY - rect.top - 14, 180, 760)
}
function stopResize(){
  window.removeEventListener('pointermove', onSidebarResize)
  window.removeEventListener('pointermove', onJsonResize)
  document.body.classList.remove('is-dragging-panel')
  resizing.value=null
  activeJsonPreview=null
}
onMounted(async()=>{ await tick(); timer=setInterval(tick,1000) })
onUnmounted(()=>{ clearInterval(timer); stopResize() })
</script>
