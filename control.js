// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 BOBI SAS, France
// Auteur : Cyril Mazouer, pour le compte de BOBI SAS
// Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

// Éditeur d'encodeur streamer — monté par le shell I/O via
// window.MXLPlugins.streamer.mount(el, vmid, ctx). Une instance montée à la fois.
// Porté à l'identique de l'ancienne page streams.html (encodage, pistes audio,
// destinations UDP/SRT/WebRTC, preview WHEP, liens clients). État par-vmid via
// GET /api/streams/<vmid> ; sauvegarde POST /api/streams/<vmid> ; polling 5 s.
window.MXLPlugins = window.MXLPlugins || {};
window.MXLPlugins.streamer = (function () {
    let EL = null, VMID = null, TOAST = () => {}, pollTimer = null, firstRender = true, lastC = null, _hostname = '';
    const T = (k) => (window.t ? window.t(k) : k);   // i18n (catalogue plugin.streamer.*)

    const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
        '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

    const CODECS = [['h264','H.264'],['h265','H.265 / HEVC']];
    const PRESETS = ['ultrafast','superfast','veryfast','faster','fast','medium'];
    const CHROMA = [['420','4:2:0'],['422','4:2:2'],['444','4:4:4']];
    const PRIMARIES = [['','auto'],['bt709','BT.709'],['bt2020','BT.2020'],['smpte170m','BT.601'],['bt470bg','BT.470BG']];
    const TRC = [['','auto'],['bt709','BT.709'],['bt2020-10','BT.2020-10'],['smpte2084','PQ (HDR)'],['arib-std-b67','HLG']];
    const SPACE = [['','auto'],['bt709','BT.709'],['bt2020nc','BT.2020 NC'],['smpte170m','BT.601']];
    const COLORIMETRY_MAP = {
        '709':{p:'bt709',t:'bt709',s:'bt709'},
        '2020':{p:'bt2020',t:'bt2020-10',s:'bt2020nc'},
        '2020pq':{p:'bt2020',t:'smpte2084',s:'bt2020nc'},
        '2020hlg':{p:'bt2020',t:'arib-std-b67',s:'bt2020nc'},
        '601':{p:'smpte170m',t:'bt709',s:'smpte170m'},
    };

    function opt(list, val){ return list.map(o => {
        const [v,l] = Array.isArray(o) ? o : [o,o];
        return `<option value="${esc(v)}" ${v==val?'selected':''}>${esc(l)}</option>`;
    }).join(''); }
    function num(id, label, val, attrs=''){ return `<label class="fld"><span>${esc(label)}</span>
        <input type="number" data-f="${id}" value="${val==null?'':esc(val)}" ${attrs}></label>`; }
    function txt(id, label, val, ph=''){ return `<label class="fld"><span>${esc(label)}</span>
        <input type="text" data-f="${id}" value="${val==null?'':esc(val)}" placeholder="${esc(ph)}"></label>`; }

    // ─── Destinations ───
    function destRow(d){
        d = d || {type:'udp'}; const t = d.type || 'udp';
        return `<div class="dest-row" data-dest>
            <label class="fld"><span>${esc(T('plugin.streamer.type'))}</span>
                <select data-f="type" onchange="__streamer.onDestType(this)">
                    <option value="udp" ${t=='udp'?'selected':''}>UDP</option>
                    <option value="srt" ${t=='srt'?'selected':''}>SRT</option>
                    <option value="webrtc" ${t=='webrtc'?'selected':''}>WebRTC</option>
                </select></label>
            <div class="dest-fields">${destFields(d)}</div>
            <button class="btn-ghost-icon" onclick="this.closest('[data-dest]').remove()" aria-label="${esc(T('plugin.streamer.del_dest'))}" title="${esc(T('plugin.streamer.delete'))}">✕</button>
        </div>`;
    }
    function destFields(d){
        const t = d.type || 'udp';
        if(t=='udp') return txt('host',T('plugin.streamer.host'),d.host,'192.0.2.21')+num('port',T('plugin.streamer.port'),d.port==null?9000:d.port);
        if(t=='srt') return txt('host',T('plugin.streamer.host'),d.host,'192.0.2.30')+num('port',T('plugin.streamer.port'),d.port==null?9001:d.port)
            +num('latency_ms',T('plugin.streamer.latency_ms'),d.latency_ms==null?120:d.latency_ms)
            +txt('passphrase',T('plugin.streamer.passphrase'),d.passphrase)+txt('streamid',T('plugin.streamer.streamid'),d.streamid);
        // Actif par défaut pour une nouvelle destination (enabled undefined) ; on
        // ne décoche que si enabled vaut explicitement false (destination existante).
        if(t=='webrtc') return txt('path',T('plugin.streamer.path'),d.path,'stream-221')
            +`<label class="switch" style="align-self:flex-end;padding-bottom:6px"><input type="checkbox" data-f="enabled" ${d.enabled!==false?'checked':''}><span>${esc(T('plugin.streamer.broadcast'))}</span></label>`;
        return '';
    }
    function _autoWebrtcPath(){
        const base = (_hostname || 'stream').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '') || 'stream';
        const used = new Set([...EL.querySelectorAll('.dest-fields [data-f="path"]')].map(i => i.value.trim()).filter(Boolean));
        if (!used.has(base)) return base;
        for (let i = 2; i < 20; i++) { const c = `${base}-${i}`; if (!used.has(c)) return c; }
        return base;
    }
    function onDestType(sel){
        const destDiv = sel.closest('[data-dest]');
        destDiv.querySelector('.dest-fields').innerHTML = destFields({type: sel.value});
        if (sel.value === 'webrtc') {
            const pathInput = destDiv.querySelector('[data-f="path"]');
            if (pathInput && !pathInput.value.trim()) pathInput.value = _autoWebrtcPath();
        }
    }
    function readDest(row){
        const t = row.querySelector('[data-f=type]').value;
        const get = f => { const el = row.querySelector(`.dest-fields [data-f="${f}"]`); if(!el) return undefined;
            return el.type==='checkbox' ? el.checked : el.value; };
        if(t=='udp') return {type:'udp', host:get('host')||'', port:parseInt(get('port'))||9000};
        if(t=='srt') return {type:'srt', host:get('host')||'', port:parseInt(get('port'))||9001,
            latency_ms:parseInt(get('latency_ms'))||120, passphrase:get('passphrase')||'', streamid:get('streamid')||''};
        if(t=='webrtc') return {type:'webrtc', path:(get('path')||'').trim(), enabled:!!get('enabled')};
        return {type:t};
    }
    function addDest(){ EL.querySelector('[data-dests]').insertAdjacentHTML('beforeend', destRow({type:'udp'})); }

    // ─── Pistes audio (mapping canaux 8→sortie) ───
    function chSel(val){ let o=''; for(let i=0;i<8;i++) o+=`<option value="${i}" ${i==val?'selected':''}>${i+1}</option>`; return `<select data-ach>${o}</select>`; }
    function chanFields(ch, stereo){
        const c0=(ch[0]!=null?ch[0]:0), c1=(ch[1]!=null?ch[1]:1);
        if(stereo) return `<label class="fld"><span>${esc(T('plugin.streamer.chan_l'))}</span>${chSel(c0)}</label><label class="fld"><span>${esc(T('plugin.streamer.chan_r'))}</span>${chSel(c1)}</label>`;
        return `<label class="fld"><span>${esc(T('plugin.streamer.chan'))}</span>${chSel(c0)}</label>`;
    }
    function audioTrackRow(t){
        const ch=(t&&t.channels)||[0]; const stereo=ch.length>=2;
        return `<div class="dest-row" data-atrack>
            <label class="fld"><span>${esc(T('plugin.streamer.type'))}</span>
                <select data-alayout onchange="__streamer.onATrackLayout(this)">
                    <option value="mono" ${!stereo?'selected':''}>${esc(T('plugin.streamer.mono'))}</option>
                    <option value="stereo" ${stereo?'selected':''}>${esc(T('plugin.streamer.stereo'))}</option>
                </select></label>
            <div class="dest-fields atrack-chans">${chanFields(ch,stereo)}</div>
            <button class="btn-ghost-icon" onclick="this.closest('[data-atrack]').remove()" aria-label="${esc(T('plugin.streamer.del_track'))}" title="${esc(T('plugin.streamer.delete'))}">✕</button>
        </div>`;
    }
    function onATrackLayout(sel){ const stereo=sel.value=='stereo';
        sel.closest('[data-atrack]').querySelector('.atrack-chans').innerHTML=chanFields(stereo?[0,1]:[0],stereo); }
    function addATrack(){ EL.querySelector('[data-atracks]').insertAdjacentHTML('beforeend', audioTrackRow({channels:[0,1]})); }
    function readATrack(row){
        const chs=[...row.querySelectorAll('.atrack-chans [data-ach]')].map(s=>parseInt(s.value)).filter(n=>!isNaN(n));
        return {channels:chs};
    }

    function liveFor(c){ const m={}; (c.live&&c.live.destinations||[]).forEach(d=>{ m[d.target]=d.up; }); return m; }

    // ─── Métriques live ───
    function fmtKbps(k){ return k>=1000?(k/1000).toFixed(2)+' Mb/s':Math.round(k)+' kb/s'; }
    function bitrateHtml(c){
        const real=(c.live&&c.live.out_bitrate_kbps>0)?fmtKbps(c.live.out_bitrate_kbps):'—';
        const v=c.params.video||{}, a=c.params.audio||{};
        let cfg=T('plugin.streamer.video')+' '+esc(v.bitrate||'?'); if(a.enabled) cfg+=' · '+T('plugin.streamer.audio')+' '+esc(a.bitrate||'?');
        return `${T('plugin.streamer.bitrate_real')} <code>${real}</code> · ${T('plugin.streamer.bitrate_cfg')} ${cfg}`;
    }
    function signalHtml(c){
        const L=c.live||{}; const iw=L.in_width||0, ih=L.in_height||0;
        const ifps=(L.fps!=null&&L.fps>0)?Math.round(Number(L.fps)):0;
        // ENTRELACÉ : in_width/in_height sont les dims de TRAME (1920×1080i, jamais 540) et le
        // balayage vient du flow_def MXL du producteur. La sortie, elle, est DÉSENTRELACÉE (bwdif).
        const iscan=(L.in_scan==='i')?'i':'', ifo=L.in_field_order||'';
        const inTxt=(iw&&ih?`${iw}×${ih}${iscan}`:T('plugin.streamer.res_unknown'))
                    +(iscan&&ifo?` ${ifo}`:'')+(ifps?` @${ifps}`:'');
        const deint=iscan?` <span style="color:var(--text-muted)">· ${esc(T('plugin.streamer.deinterlaced'))}</span>`:'';
        const ow=L.out_width||0, oh=L.out_height||0, ofps=L.out_fps||0;
        if(!ow && !oh && !ofps) return `${T('plugin.streamer.signal_in')} <code>${esc(inTxt)}</code>${deint} <span style="color:var(--text-muted)">· ${esc(T('plugin.streamer.out_same'))}</span>`;
        const outW=ow||iw, outH=oh||ih, outF=ofps||ifps;
        const outTxt=(outW&&outH?`${outW}×${outH}`:'?')+(outF?` @${outF}`:'');
        const scaleAdapt=ow&&oh&&iw&&ih&&(ow!==iw||oh!==ih), fpsAdapt=ofps&&ifps&&(ofps!==ifps);
        if(scaleAdapt||fpsAdapt){ const what=[scaleAdapt?T('plugin.streamer.resized'):null, fpsAdapt?T('plugin.streamer.resampled'):null].filter(Boolean).join(' + ');
            return `${T('plugin.streamer.signal_in')} <code>${esc(inTxt)}</code>${deint} <span style="color:var(--status-warning-fg)">→ ${esc(what)} ${esc(T('plugin.streamer.in_to'))}</span> <code>${esc(outTxt)}</code>`; }
        return `${T('plugin.streamer.signal_in')} <code>${esc(inTxt)}</code>${deint} <span style="color:var(--text-muted)">${esc(T('plugin.streamer.out_to'))} <code>${esc(outTxt)}</code> ${esc(T('plugin.streamer.out_identical'))}</span>`;
    }
    function latencyHtml(c){
        const L=(c.live&&c.live.inputs_latency_ms)||{}; const fmt=v=>(v==null?'—':v+' ms');
        const vs=c.params.shm_name, as=c.params.audio_shm, a=c.params.audio||{};
        let s=`${T('plugin.streamer.latency_in')} <code>${fmt(vs?L[vs]:null)}</code>`;
        if(a.enabled && as) s+=` · ${T('plugin.streamer.audio')} <code>${fmt(L[as])}</code>`;
        return s;
    }

    function hotHelp(hot){
        return hot ? T('plugin.streamer.hot_help_on') : T('plugin.streamer.hot_help_off');
    }
    function onHotMode(sel){ const help=EL.querySelector('[data-hot-help]'); if(help) help.textContent=hotHelp(sel.value==='1'); }

    // ─── Selects de format (Réglages → Vidéo, cache global) ───
    window._videoFormats = (window._videoFormats!==undefined)?window._videoFormats:null;
    window._videoFormatDefault = window._videoFormatDefault || '';
    async function loadVideoFormats(){
        if(window._videoFormats!==null) return window._videoFormats;
        try{
            const r=await fetch('/api/settings'); if(!r.ok){ window._videoFormats=[]; return []; }
            const s=await r.json(); window._videoFormatDefault=s.video_format_default||'';
            window._videoFormats=(s.video_formats||'').split('\n').map(l=>l.trim()).filter(Boolean).map(l=>{
                const p=l.split(';').map(x=>x.trim());
                return {label:p[0]||'', w:parseInt(p[1])||0, h:parseInt(p[2])||0,
                    fps:parseFloat(p[3])||25, scan:(p[4]||'p').toLowerCase()==='i'?'i':'p',
                    chroma:['420','422','444'].includes(p[5])?p[5]:'422',
                    bit_depth:[8,10,12].includes(parseInt(p[6]))?parseInt(p[6]):10,
                    colorimetry:(p[7]||'709').toLowerCase()};
            }).filter(f=>f.label&&f.w&&f.h);
        }catch(e){ window._videoFormats=[]; }
        return window._videoFormats;
    }
    function populateFormatSelect(){
        const sel=EL.querySelector('.dp-format-preset'); if(!sel||!window._videoFormats) return;
        const opts=window._videoFormats.map(f=>
            `<option value="${f.w}x${f.h}" data-label="${esc(f.label)}" data-w="${f.w}" data-h="${f.h}" data-fps="${f.fps}" data-scan="${f.scan}" data-chroma="${f.chroma}" data-bd="${f.bit_depth}" data-colorimetry="${f.colorimetry}">${esc(f.label)}</option>`);
        sel.innerHTML='<option value="">'+esc(T('plugin.streamer.follow_input'))+'</option>'+opts.join('');
    }
    function _getFormatValues(){
        const sel=EL.querySelector('.dp-format-preset'); if(!sel||!sel.value) return {w:0,h:0,fps:0};
        const o=sel.selectedOptions[0];
        return {w:parseInt(o.dataset.w)||0, h:parseInt(o.dataset.h)||0, fps:parseFloat(o.dataset.fps)||0,
            chroma:o.dataset.chroma||'', bit_depth:parseInt(o.dataset.bd)||0, colorimetry:o.dataset.colorimetry||''};
    }
    function onStreamFormatChange(sel){
        const o=sel.selectedOptions[0]; if(!o||!sel.value) return;
        const setF=(f,val)=>{ const el=EL.querySelector(`[data-f="${f}"]`); if(el&&val!=null) el.value=val; };
        if(o.dataset.chroma) setF('video.chroma', o.dataset.chroma);
        const c=COLORIMETRY_MAP[(o.dataset.colorimetry||'').toLowerCase()];
        if(c){ setF('video.color_primaries',c.p); setF('video.color_trc',c.t); setF('video.colorspace',c.s); }
    }
    async function setupFormatSelect(c){
        await loadVideoFormats();
        const sel=EL.querySelector('.dp-format-preset'); if(!sel) return;
        populateFormatSelect();
        const v=c.params.video||{};
        const w=parseInt(v.width)||0, h=parseInt(v.height)||0, fps=parseFloat(v.fps)||0;
        if(!w || !h){ sel.value=''; return; }
        const match=[...sel.options].find(o=>o.dataset.w
            && parseInt(o.dataset.w)===w && parseInt(o.dataset.h)===h && (!fps || parseFloat(o.dataset.fps)===fps));
        if(match){ sel.value=match.value; }
        else { const o=document.createElement('option');
            o.value='custom'; o.dataset.w=w; o.dataset.h=h; o.dataset.fps=fps||25; o.dataset.scan='p';
            o.textContent=T('plugin.streamer.custom_fmt').replace('{dim}', `${w}×${h}${fps?(' @'+fps):''}`);
            sel.insertBefore(o, sel.options[1]||null); sel.value='custom'; }
    }

    // ─── Preview WebRTC ───
    function previewEnabled(){ return localStorage.getItem('mxl.streams.preview.'+VMID)!=='0'; }
    function previewBtnLabel(on){ return on?T('plugin.streamer.preview_hide'):T('plugin.streamer.preview_show'); }
    function hasWebrtc(c){ return (c.params.destinations||[]).some(d=>d.type=='webrtc' && d.enabled); }
    function previewIsActive(c, d){ const ups=liveFor(c);
        return d.embed_url && (ups[d.embed_url]!==undefined ? ups[d.embed_url] : !!(c.live && c.live.fps>0)); }
    function mtxPlayerUrl(embed, controls){ if(!embed) return embed;
        const base=embed.endsWith('/')?embed:embed+'/';
        return base+`?controls=${controls?'true':'false'}&muted=true&autoplay=true&playsinline=true`; }
    function controlsOn(){ return localStorage.getItem('mxl.streams.controls.'+VMID)==='1'; }
    function toggleControls(){
        localStorage.setItem('mxl.streams.controls.'+VMID, controlsOn()?'0':'1');
        const pv=EL.querySelector('[data-webrtc-preview]');
        if(pv && lastC){ pv.innerHTML=previewHtml(lastC); pv.dataset.sig=previewSig(lastC); }
    }
    function previewSig(c){
        if(!previewEnabled()) return '"off"';
        const wrtc=(c.params.destinations||[]).filter(d=>d.type=='webrtc' && d.enabled);
        return JSON.stringify(wrtc.map(d=>[d.path||'', d.embed_url||'', previewIsActive(c,d)?1:0]));
    }
    function previewHtml(c){
        if(!previewEnabled()) return '';
        const wrtc=(c.params.destinations||[]).filter(d=>d.type=='webrtc' && d.enabled);
        if(!wrtc.length) return '';
        return wrtc.map(d=>{
            if(previewIsActive(c,d)){ const con=controlsOn();
                return `<div>
                    <div class="sect-title" style="display:flex;align-items:center;gap:8px">
                        <span>${esc(T('plugin.streamer.preview_title'))} ${esc(d.path)}</span>
                        <button class="btn-text-action" style="margin-left:auto;text-transform:none" onclick="__streamer.toggleControls()">${con?esc(T('plugin.streamer.controls_hide')):esc(T('plugin.streamer.controls_show'))}</button>
                    </div>
                    <iframe src="${esc(mtxPlayerUrl(d.embed_url, con))}" title="${esc(T('plugin.streamer.preview_iframe').replace('{path}', d.path))}" allow="autoplay" allowfullscreen></iframe></div>`; }
            if(d.embed_url) return `<div class="preview-note">${esc(T('plugin.streamer.preview_off').replace('{path}', d.path))}</div>`;
            return `<div class="preview-note">${esc(T('plugin.streamer.preview_no_gw').replace('{path}', d.path))}</div>`;
        }).join('');
    }
    function togglePreview(){
        const on=!previewEnabled();
        localStorage.setItem('mxl.streams.preview.'+VMID, on?'1':'0');
        const btn=EL.querySelector('[data-preview-toggle]');
        if(btn){ btn.textContent=previewBtnLabel(on); btn.classList.toggle('is-open', on); btn.setAttribute('aria-expanded', on?'true':'false'); }
        const pv=EL.querySelector('[data-webrtc-preview]');
        if(pv && lastC){ pv.innerHTML=previewHtml(lastC); pv.dataset.sig=previewSig(lastC); }
    }

    // ─── Liens clients (pages publiques WebRTC) ───
    function shareLinkRow(l){
        return `<div class="dest-row" data-share-token="${esc(l.token)}" style="grid-template-columns:1fr auto auto">
            <div style="min-width:0">
                <div style="font-weight:600">${esc(l.title||'—')}</div>
                ${l.note?`<div class="meta">${esc(l.note)}</div>`:''}
                <div class="meta" style="word-break:break-all"><code>${esc(l.url)}</code></div>
            </div>
            <button class="btn-text-action" title="${esc(T('plugin.streamer.copy_link'))}" onclick="__streamer.copyShareLink('${esc(l.url)}')">${esc(T('plugin.streamer.copy'))}</button>
            <button class="btn-ghost-icon" title="${esc(T('plugin.streamer.del_link'))}" aria-label="${esc(T('plugin.streamer.del_link'))}" onclick="__streamer.deleteShareLink('${esc(l.token)}')">✕</button>
        </div>`;
    }
    function renderShareList(links){
        const box=EL.querySelector('[data-share-list]'); if(!box) return;
        box.innerHTML=(links&&links.length)?links.map(shareLinkRow).join(''):'<div class="meta">'+esc(T('plugin.streamer.no_links'))+'</div>';
    }
    async function loadShareLinks(){
        try{ const r=await fetch('/api/streams/'+VMID+'/share'); if(!r.ok) return; renderShareList(await r.json()); }catch(e){}
    }
    async function createShareLink(){
        const g=f=>EL.querySelector(`[data-f="${f}"]`);
        const body={ title:(g('share.title')?g('share.title').value:'').trim(), note:(g('share.note')?g('share.note').value:'').trim() };
        try{
            const r=await fetch('/api/streams/'+VMID+'/share',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
            const j=await r.json();
            if(!r.ok){ TOAST(T('plugin.streamer.error')+' '+(j.error||T('plugin.streamer.error_unknown')),'error'); return; }
            TOAST(T('plugin.streamer.link_created'),'info'); copyShareLink(j.url); loadShareLinks();
        }catch(e){ TOAST(T('plugin.streamer.net_error')+' '+e.message,'error'); }
    }
    async function deleteShareLink(token){
        if(!confirm(T('plugin.streamer.del_link_confirm'))) return;
        try{ const r=await fetch('/api/share/'+encodeURIComponent(token),{method:'DELETE'});
            if(!r.ok){ TOAST(T('plugin.streamer.del_link_fail'),'error'); return; }
            TOAST(T('plugin.streamer.link_deleted'),'info'); loadShareLinks();
        }catch(e){ TOAST(T('plugin.streamer.net_error')+' '+e.message,'error'); }
    }
    function copyShareLink(url){
        if(navigator.clipboard && navigator.clipboard.writeText)
            navigator.clipboard.writeText(url).then(()=>TOAST(T('plugin.streamer.link_copied'),'info'), ()=>TOAST(T('plugin.streamer.link_label')+' '+url,'info'));
        else TOAST(T('plugin.streamer.link_label')+' '+url,'info');
    }

    // ─── Sauvegarde ───
    function saveStream(){
        const g=f=>EL.querySelector(`[data-f="${f}"]`);
        const gv=f=>{ const el=g(f); return el?(el.type==='checkbox'?el.checked:el.value):undefined; };
        const f=_getFormatValues();
        const body={
            hot_input: gv('hot_input')==='1',
            video:{ codec:gv('video.codec'), bitrate:gv('video.bitrate'), preset:gv('video.preset'),
                gop:parseInt(gv('video.gop'))||0, width:f.w, height:f.h, fps:f.fps, chroma:gv('video.chroma'),
                color_primaries:gv('video.color_primaries'), color_trc:gv('video.color_trc'), colorspace:gv('video.colorspace') },
            audio:{ enabled:gv('audio.enabled'), bitrate:gv('audio.bitrate'),
                tracks:[...EL.querySelectorAll('[data-atrack]')].map(readATrack).filter(t=>t.channels.length) },
            destinations:[...EL.querySelectorAll('[data-dest]')].map(readDest),
        };
        fetch('/api/streams/'+VMID,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})
            .then(r=>r.json().then(j=>({ok:r.ok,j})))
            .then(({ok,j})=>{
                if(!ok){ TOAST(T('plugin.streamer.error')+' '+(j.error||T('plugin.streamer.error_unknown')),'error'); return; }
                if(j.status==='remap_a_chaud'){ TOAST(T('plugin.streamer.audio_remapped'),'info'); setTimeout(loadOne, 500); }
                else { TOAST(T('plugin.streamer.deploying'),'info'); firstRender=true; setTimeout(loadOne, 2500); }
            })
            .catch(e=>TOAST(T('plugin.streamer.net_error')+' '+e.message,'error'));
    }

    // ─── Rendu carte + live update ───
    function renderInner(c){ _hostname = c.hostname || '';
        const p=c.params, v=p.video||{}, a=p.audio||{};
        const fps=(c.live&&c.live.fps!=null)?Number(c.live.fps).toFixed(1):'—';
        const fpsCol=(c.live&&c.live.fps>=24)?'var(--status-running-fg)':(c.live&&c.live.fps>0)?'var(--status-warning-fg)':'var(--status-stopped-fg)';
        EL.innerHTML=`
        <div class="meta" style="margin-bottom:8px;display:flex;align-items:center;gap:8px">
            <span style="font-family:var(--font-mono);color:${fpsCol}" data-live-fps>${fps} fps</span>
            <span>${esc(T('plugin.streamer.input'))} <code>${esc(p.shm_name||'—')}</code> · IP ${esc(c.ip||'—')}</span>
            ${p.shm_name ? `<button class="btn-text-action" onclick="MXLMonitor.send('${esc(p.shm_name)}','${esc(c.hostname||'')} ${esc(T('plugin.streamer.input_suffix'))}')">${esc(T('plugin.streamer.monitoring'))}</button>` : ''}
        </div>
        <div class="meta" style="margin-bottom:8px" data-bitrate>${bitrateHtml(c)}</div>
        <div class="meta" style="margin-bottom:8px" data-signal>${signalHtml(c)}</div>
        <div class="meta" style="margin-bottom:8px" data-latency>${latencyHtml(c)}</div>

        <div class="sect">
            <div class="sect-title">${esc(T('plugin.streamer.encoding'))}</div>
            <div class="fld-grid">
                <label class="fld"><span>${esc(T('plugin.streamer.video_codec'))}</span><select data-f="video.codec">${opt(CODECS,v.codec)}</select></label>
                ${txt('video.bitrate',T('plugin.streamer.bitrate'),v.bitrate,'4M')}
                <label class="fld"><span>${esc(T('plugin.streamer.preset'))}</span><select data-f="video.preset">${opt(PRESETS,v.preset)}</select></label>
                ${num('video.gop',T('plugin.streamer.gop'),v.gop)}
                <label class="fld"><span>${esc(T('plugin.streamer.chroma'))}</span><select data-f="video.chroma">${opt(CHROMA,v.chroma||'422')}</select></label>
                <label class="fld" style="grid-column:span 2"><span>${esc(T('plugin.streamer.src_mode'))}</span>
                    <select data-f="hot_input" onchange="__streamer.onHotMode(this)">
                        <option value="0" ${p.hot_input?'':'selected'}>${esc(T('plugin.streamer.src_auto'))}</option>
                        <option value="1" ${p.hot_input?'selected':''}>${esc(T('plugin.streamer.src_hot'))}</option>
                    </select></label>
                <label class="fld" style="grid-column:span 2"><span>${esc(T('plugin.streamer.out_format'))}</span>
                    <select class="dp-format-preset" onchange="__streamer.onStreamFormatChange(this)"><option value="">${esc(T('plugin.streamer.follow_input'))}</option></select></label>
            </div>
            <div class="meta" style="margin-top:6px" data-hot-help>${hotHelp(!!p.hot_input)}</div>
            <div class="sect-title" style="margin-top:10px">${esc(T('plugin.streamer.colorimetry'))}</div>
            <div class="fld-grid">
                <label class="fld"><span>${esc(T('plugin.streamer.primaries'))}</span><select data-f="video.color_primaries">${opt(PRIMARIES,v.color_primaries||'')}</select></label>
                <label class="fld"><span>${esc(T('plugin.streamer.trc'))}</span><select data-f="video.color_trc">${opt(TRC,v.color_trc||'')}</select></label>
                <label class="fld"><span>${esc(T('plugin.streamer.space'))}</span><select data-f="video.colorspace">${opt(SPACE,v.colorspace||'')}</select></label>
            </div>
            <div class="meta" style="margin-top:6px">${esc(T('plugin.streamer.auto_note'))}</div>
        </div>

        <div class="sect">
            <div class="sect-title">${esc(T('plugin.streamer.audio'))}</div>
            <label class="switch" style="margin-bottom:10px"><input type="checkbox" data-f="audio.enabled" ${a.enabled?'checked':''}><span>${esc(T('plugin.streamer.audio_enable'))}</span></label>
            <div class="fld-grid">
                ${txt('audio.bitrate',T('plugin.streamer.bitrate_track'),a.bitrate,'128k')}
                <label class="fld"><span>${esc(T('plugin.streamer.audio_shm'))}</span>
                    <input type="text" value="${esc(p.audio_shm||'')}" readonly placeholder="${esc(T('plugin.streamer.audio_shm_ph'))}" style="opacity:.7"></label>
            </div>
            <div class="meta" style="margin:6px 0">${esc(T('plugin.streamer.audio_hint'))}</div>
            <div data-atracks>${(a.tracks||[]).map(audioTrackRow).join('')}</div>
            <button class="btn" onclick="__streamer.addATrack()">${esc(T('plugin.streamer.add_track'))}</button>
        </div>

        <div class="sect">
            <div class="sect-title">${esc(T('plugin.streamer.destinations'))}</div>
            <div data-dests>${(p.destinations||[]).map(destRow).join('')}</div>
            <button class="btn" onclick="__streamer.addDest()">${esc(T('plugin.streamer.add_dest'))}</button>
        </div>

        ${hasWebrtc(c) ? `<div class="st-prev-toggle-row">
            <button type="button" class="st-prev-toggle ${previewEnabled()?'is-open':''}" data-preview-toggle
                    aria-expanded="${previewEnabled()?'true':'false'}" onclick="__streamer.togglePreview()">${previewBtnLabel(previewEnabled())}</button>
        </div>` : ''}
        <div data-webrtc-preview class="webrtc-preview" data-sig="${esc(previewSig(c))}">${previewHtml(c)}</div>

        ${hasWebrtc(c) ? `<div class="sect" data-share-sect>
            <div class="sect-title">${esc(T('plugin.streamer.share_title'))}</div>
            <div class="meta" style="margin-bottom:8px">${esc(T('plugin.streamer.share_hint'))}</div>
            <div class="fld-grid" style="margin-bottom:8px">
                ${txt('share.title',T('plugin.streamer.share_name'),c.hostname||'')}
                ${txt('share.note',T('plugin.streamer.share_note'),'',T('plugin.streamer.share_note_ph'))}
            </div>
            <button class="btn btn-blue" onclick="__streamer.createShareLink()">${esc(T('plugin.streamer.share_create'))}</button>
            <div data-share-list style="margin-top:10px"></div>
        </div>` : ''}

        <div style="display:flex;justify-content:flex-end;margin-top:10px">
            <button class="btn btn-blue" onclick="__streamer.saveStream()">${esc(T('plugin.streamer.save_deploy'))}</button>
        </div>`;
    }

    function liveUpdate(c){
        const set=(sel,html)=>{ const el=EL.querySelector(sel); if(el) el.innerHTML=html; };
        const fpsEl=EL.querySelector('[data-live-fps]');
        if(fpsEl){ const f=(c.live&&c.live.fps!=null)?Number(c.live.fps).toFixed(1):'—';
            fpsEl.textContent=f+' fps';
            fpsEl.style.color=(c.live&&c.live.fps>=24)?'var(--status-running-fg)':(c.live&&c.live.fps>0)?'var(--status-warning-fg)':'var(--status-stopped-fg)'; }
        set('[data-bitrate]', bitrateHtml(c)); set('[data-signal]', signalHtml(c)); set('[data-latency]', latencyHtml(c));
        const pv=EL.querySelector('[data-webrtc-preview]');
        if(pv){ const sig=previewSig(c); if(pv.dataset.sig!==sig){ pv.innerHTML=previewHtml(c); pv.dataset.sig=sig; } }
    }

    async function loadOne(){
        let c;
        try { const r=await fetch('/api/streams/'+VMID); if(!r.ok) throw new Error('HTTP '+r.status); c=await r.json(); }
        catch(e){ if(firstRender && EL) EL.innerHTML='<div class="meta">'+esc(T('plugin.streamer.unavailable'))+'</div>'; return; }
        lastC=c;
        if(firstRender){
            renderInner(c); firstRender=false;
            setupFormatSelect(c);
            if(hasWebrtc(c)) loadShareLinks();
        } else {
            liveUpdate(c);
        }
    }

    function mount(el, vmid, ctx){
        EL = el.querySelector('.st-body') || el;
        VMID = vmid; TOAST = (ctx && ctx.toast) || (()=>{});
        firstRender = true; lastC = null;
        // Traduit le placeholder statique du fragment (servi sans Jinja).
        el.querySelectorAll('[data-i18n]').forEach(n => { n.textContent = T(n.dataset.i18n); });
        loadOne();
        if(pollTimer) clearInterval(pollTimer);
        pollTimer = setInterval(loadOne, 5000);
    }
    function unmount(){
        if(pollTimer){ clearInterval(pollTimer); pollTimer=null; }
        EL = null; VMID = null; lastC = null;
    }

    const exp = {mount, unmount, onDestType, addDest, addATrack, onATrackLayout, onHotMode,
        onStreamFormatChange, togglePreview, toggleControls, saveStream,
        createShareLink, deleteShareLink, copyShareLink};
    window.__streamer = exp;
    return exp;
})();
