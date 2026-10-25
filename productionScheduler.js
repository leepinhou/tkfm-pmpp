/**
 * productionScheduler.js
 * 遊戲排程控制器（Web Worker 背景運算 + 深層斷點續傳 + IndexedDB 持久化）
 * 版本號：1.1.0
 */
(function (global) {
  const SCHEDULER_VERSION = "1.1.0";

  // ==========================================
  // Web Worker 核心運算代碼（封裝於 Blob 中執行）
  // ==========================================
  const workerCode = `
    const SCHEDULER_VERSION = "${SCHEDULER_VERSION}";
    const DB_NAME = "GameProductionSchedulerDB";
    const STORE_NAME = "checkpoints";

    let dbInstance = null;
    let stopRequested = false;
    let currentTask = null;

    // -------------------------------------------------------------
    // IndexedDB 輕量底層封裝
    // -------------------------------------------------------------
    function openDB() {
      if (dbInstance) return Promise.resolve(dbInstance);
      return new Promise((resolve, reject) => {
        const req = indexedDB.open(DB_NAME, 1);
        req.onupgradeneeded = (e) => {
          const db = e.target.result;
          if (!db.objectStoreNames.contains(STORE_NAME)) {
            db.createObjectStore(STORE_NAME, { keyPath: "key" });
          }
        };
        req.onsuccess = (e) => {
          dbInstance = e.target.result;
          resolve(dbInstance);
        };
        req.onerror = () => reject(req.error);
      });
    }

    async function getCheckpoint(key) {
      try {
        const db = await openDB();
        return new Promise((resolve) => {
          const tx = db.transaction(STORE_NAME, "readonly");
          const store = tx.objectStore(STORE_NAME);
          const req = store.get(key);
          req.onsuccess = () => resolve(req.result || null);
          req.onerror = () => resolve(null);
        });
      } catch (e) {
        return null;
      }
    }

    async function saveCheckpoint(record) {
      try {
        const db = await openDB();
        return new Promise((resolve) => {
          const tx = db.transaction(STORE_NAME, "readwrite");
          const store = tx.objectStore(STORE_NAME);
          store.put(record);
          tx.oncomplete = () => resolve(true);
          tx.onerror = () => resolve(false);
        });
      } catch (e) {
        return false;
      }
    }

    async function checkAndCleanOldVersions() {
      try {
        const meta = await getCheckpoint("__META_VERSION__");
        if (!meta || meta.version !== SCHEDULER_VERSION) {
          const db = await openDB();
          await new Promise((resolve) => {
            const tx = db.transaction(STORE_NAME, "readwrite");
            const store = tx.objectStore(STORE_NAME);
            store.clear();
            tx.oncomplete = () => resolve(true);
          });
          await saveCheckpoint({ key: "__META_VERSION__", version: SCHEDULER_VERSION });
        }
      } catch (e) {}
    }

    // -------------------------------------------------------------
    // 抽象同構 Key 生成與素材雙向映射
    // -------------------------------------------------------------
    function buildIsomorphicContext(materials) {
      const basicList = [];
      const advList = [];

      for (const k of Object.keys(materials)) {
        if (materials[k] > 0) {
          if (k.startsWith("+")) advList.push({ name: k, count: materials[k] });
          else basicList.push({ name: k, count: materials[k] });
        }
      }

      // 嚴格按數量由小到大排序
      basicList.sort((a, b) => a.count - b.count);
      advList.sort((a, b) => a.count - b.count);

      const abstractReq = {};
      const idToConcreteName = {};
      const bNums = [];
      const aNums = [];

      basicList.forEach((item, idx) => {
        const id = "B" + idx;
        abstractReq[id] = item.count;
        idToConcreteName[id] = item.name;
        bNums.push(item.count);
      });

      advList.forEach((item, idx) => {
        const id = "A" + idx;
        abstractReq[id] = item.count;
        idToConcreteName[id] = item.name;
        aNums.push(item.count);
      });

      const cacheKey = "SCHED_v" + SCHEDULER_VERSION + "_B:[" + bNums.join(",") + "]_A:[" + aNums.join(",") + "]";

      return { abstractReq, idToConcreteName, cacheKey };
    }

    function restoreConcreteBatches(abstractBatches, idToName) {
      return abstractBatches.map(b => ({
        lines: b.lines.map(id => id ? (idToName[id] || "(無)") : null),
        amount: b.amount
      }));
    }

    // -------------------------------------------------------------
    // Worker 主監聽器
    // -------------------------------------------------------------
    self.onmessage = async function (e) {
      const { type, materials } = e.data;
      if (type === "START") {
        stopRequested = false;
        await checkAndCleanOldVersions();
        runScheduler(materials);
      } else if (type === "STOP") {
        stopRequested = true;
      }
    };

    async function runScheduler(materials) {
      const startTime = performance.now();
      let lastProgressTime = startTime;
      const PROGRESS_INTERVAL_MS = 150;

      const { abstractReq, idToConcreteName, cacheKey } = buildIsomorphicContext(materials);
      const inputKeys = Object.keys(abstractReq);

      if (inputKeys.length === 0) {
        self.postMessage({ type: "COMPLETE", data: null });
        return;
      }

      const totalRequired = Object.values(abstractReq).reduce((a, b) => a + b, 0);
      const allowedWaste = (3 - (totalRequired % 3)) % 3;

      const keyOrderMap = new Map(inputKeys.map((k, idx) => [k, idx]));
      const getWeight = (name) => (!name || name === "(無)") ? 0 : (name.startsWith("A") || name.startsWith("+") ? 1 : 0);
      const getOrderScore = (name) => (!name || name === "(無)") ? 999999 : (keyOrderMap.get(name) ?? 999999);

      let currentBestBatches = null;
      let minBatchesFound = Infinity;
      let maxContinuityScore = -1;
      let minMixedBatches = Infinity;
      let exploredNodes = 0;
      let currentCallStackPath = [];

      // 檢查 IndexedDB 快取斷點
      const savedCheckpoint = await getCheckpoint(cacheKey);

      if (savedCheckpoint && savedCheckpoint.bestAbstractBatches) {
        currentBestBatches = savedCheckpoint.bestAbstractBatches;
        minBatchesFound = currentBestBatches.length;
        maxContinuityScore = evaluateContinuity(currentBestBatches, getWeight, getOrderScore);
        minMixedBatches = countMixedBatches(currentBestBatches);

        // 立刻將上次挖到的最好解映射並發送給 UI（0 毫秒呈現）
        emitSolutionUpdate("SEARCHING", "已載入歷史最佳進度，從斷點繼續深挖中...");

        // 若歷史記錄已證明最優，直接收工
        if (savedCheckpoint.isOptimal) {
          const finalConcrete = restoreConcreteBatches(currentBestBatches, idToConcreteName);
          const formatted = formatResult(finalConcrete, getWeight, getOrderScore, materials, startTime, "OPTIMAL", 0, "已確認為全域最優解（快取命中）！", allowedWaste);
          self.postMessage({ type: "COMPLETE", data: formatted });
          return;
        }
      }

      function emitSolutionUpdate(status, message) {
        if (!currentBestBatches) return;
        const concreteBatches = restoreConcreteBatches(currentBestBatches, idToConcreteName);
        const formatted = formatResult(concreteBatches, getWeight, getOrderScore, materials, startTime, status, exploredNodes, message, allowedWaste);
        self.postMessage({ type: "UPDATE", data: formatted });
      }

      function emitProgressHeartbeat(percent) {
        const now = performance.now();
        const totalSlots = currentBestBatches ? currentBestBatches.reduce((sum, b) => sum + b.lines.filter(Boolean).length, 0) : 0;
        const currentOps = currentBestBatches ? (totalSlots - maxContinuityScore) : 0;

        self.postMessage({
          type: "PROGRESS",
          data: {
            percent: Math.min(99, Math.max(0, percent)),
            nodes: exploredNodes,
            elapsedMs: Math.round(now - startTime),
            currentBestBatches: minBatchesFound === Infinity ? 0 : minBatchesFound,
            currentBestOps: currentOps,
            currentMixedBatches: minMixedBatches === Infinity ? 0 : minMixedBatches
          }
        });
      }

      // 初解保底（若快取沒有）
      if (!currentBestBatches) {
        const greedyBatches = solveGreedyFast(abstractReq, allowedWaste);
        if (greedyBatches && calculateWaste(greedyBatches) === allowedWaste) {
          currentBestBatches = greedyBatches;
          minBatchesFound = greedyBatches.length;
          maxContinuityScore = evaluateContinuity(greedyBatches, getWeight, getOrderScore);
          minMixedBatches = countMixedBatches(greedyBatches);
          emitSolutionUpdate("SEARCHING", "已生成初版排程，背景持續深度優化中...");
        }
      }

      // -------------------------------------------------------------
      // 深度 DFS 搜尋（含深層指針快進 Fast-Forward Resumption）
      // -------------------------------------------------------------
      const resumeCursor = (savedCheckpoint && savedCheckpoint.cursor) ? savedCheckpoint.cursor : null;
      let isFastForwarding = !!resumeCursor;

      const req = { ...abstractReq };
      const topActions = getCandidateActions(req, Infinity, "");
      const totalTopActions = topActions.length;

      for (let topIdx = 0; topIdx < totalTopActions; topIdx++) {
        if (stopRequested) break;

        // 頂層快進檢查
        if (isFastForwarding && resumeCursor.length > 0) {
          if (topIdx < resumeCursor[0]) continue;
          if (topIdx > resumeCursor[0]) isFastForwarding = false;
        }

        const action = topActions[topIdx];
        const currentProgress = Math.floor((topIdx / totalTopActions) * 100);

        for (const item of action.pattern) req[item] -= action.amt;
        const initialBatches = [{ lines: action.pattern, amount: action.amt }];

        currentCallStackPath = [topIdx];
        dfs(req, initialBatches, action.amt, action.pKey, 1, currentProgress);

        for (const item of action.pattern) req[item] += action.amt;
      }

      function dfs(currentReq, currentBatches, lastAmount, lastPatternKey, depth, branchProgress) {
        if (stopRequested) return;
        exploredNodes++;

        if ((exploredNodes & 4095) === 0) {
          const now = performance.now();
          if (now - lastProgressTime >= PROGRESS_INTERVAL_MS) {
            lastProgressTime = now;
            emitProgressHeartbeat(branchProgress);
          }
        }

        if (currentBatches.length >= minBatchesFound) return;

        const activeKeys = Object.keys(currentReq).filter(k => currentReq[k] > 0);

        if (activeKeys.length === 0) {
          if (allowedWaste === 0) checkAndUpdateSolution(currentBatches);
          return;
        }

        const oneStepBatch = tryOneStepClear(currentReq, activeKeys, allowedWaste);
        if (oneStepBatch) {
          const finalBatches = [...currentBatches, oneStepBatch];
          checkAndUpdateSolution(finalBatches);
          return;
        }

        const actions = getCandidateActions(currentReq, lastAmount, lastPatternKey);

        for (let actIdx = 0; actIdx < actions.length; actIdx++) {
          if (stopRequested) return;

          // 深層快進跳過
          if (isFastForwarding) {
            if (depth < resumeCursor.length) {
              const targetIdx = resumeCursor[depth];
              if (actIdx < targetIdx) continue;
              if (actIdx > targetIdx) isFastForwarding = false;
            } else {
              isFastForwarding = false;
            }
          }

          const act = actions[actIdx];
          for (const item of act.pattern) currentReq[item] -= act.amt;
          currentBatches.push({ lines: act.pattern, amount: act.amt });

          currentCallStackPath.push(actIdx);
          dfs(currentReq, currentBatches, act.amt, act.pKey, depth + 1, branchProgress);
          currentCallStackPath.pop();

          currentBatches.pop();
          for (const item of act.pattern) currentReq[item] += act.amt;
        }
      }

      function checkAndUpdateSolution(batches) {
        if (calculateWaste(batches) !== allowedWaste) return;

        const continuity = evaluateContinuity(batches, getWeight, getOrderScore);
        const mixed = countMixedBatches(batches);
        let isBetter = false;

        if (batches.length < minBatchesFound) {
          isBetter = true;
        } else if (batches.length === minBatchesFound) {
          if (continuity > maxContinuityScore) {
            isBetter = true;
          } else if (continuity === maxContinuityScore) {
            if (mixed < minMixedBatches) isBetter = true;
          }
        }

        if (isBetter) {
          minBatchesFound = batches.length;
          maxContinuityScore = continuity;
          minMixedBatches = mixed;
          currentBestBatches = batches.map(b => ({ lines: [...b.lines], amount: b.amount }));
          emitSolutionUpdate("SEARCHING", "找到更優排程：無浪費且操作次數更少！");

          // 挖到更好解時非同步存入 IndexedDB
          saveCheckpoint({
            key: cacheKey,
            version: SCHEDULER_VERSION,
            bestAbstractBatches: currentBestBatches,
            cursor: [...currentCallStackPath],
            isOptimal: false,
            updatedAt: Date.now()
          });
        }
      }

      // 中斷或完成處理
      if (stopRequested) {
        // 優雅停止：安全保存當前深層路徑指針
        await saveCheckpoint({
          key: cacheKey,
          version: SCHEDULER_VERSION,
          bestAbstractBatches: currentBestBatches,
          cursor: [...currentCallStackPath],
          isOptimal: false,
          updatedAt: Date.now()
        });
        self.postMessage({ type: "STOPPED" });
        return;
      }

      // 全域最優完成
      if (currentBestBatches) {
        await saveCheckpoint({
          key: cacheKey,
          version: SCHEDULER_VERSION,
          bestAbstractBatches: currentBestBatches,
          cursor: null,
          isOptimal: true,
          updatedAt: Date.now()
        });
        const finalConcrete = restoreConcreteBatches(currentBestBatches, idToConcreteName);
        const finalFormatted = formatResult(finalConcrete, getWeight, getOrderScore, materials, startTime, "OPTIMAL", exploredNodes, "已確認為全域最優解！", allowedWaste);
        self.postMessage({ type: "COMPLETE", data: finalFormatted });
      }
    }

    // -------------------------------------------------------------
    // 排程演算法基礎函式
    // -------------------------------------------------------------
    function countMixedBatches(batches) {
      let count = 0;
      for (const b of batches) {
        let hasBasic = false;
        let hasAdv = false;
        for (const item of b.lines) {
          if (!item || item === "(無)") continue;
          if (item.startsWith("A") || item.startsWith("+")) hasAdv = true;
          else hasBasic = true;
        }
        if (hasBasic && hasAdv) count++;
      }
      return count;
    }

    function calculateWaste(batches) {
      let waste = 0;
      for (const b of batches) {
        for (const item of b.lines) {
          if (!item || item === "(無)") waste += b.amount;
        }
      }
      return waste;
    }

    function getCandidateActions(currentReq, lastAmount, lastPatternKey) {
      const activeKeys = Object.keys(currentReq).filter(k => currentReq[k] > 0);
      const candidateAmounts = new Set();
      candidateAmounts.add(1);

      for (let i = 0; i < activeKeys.length; i++) {
        const k1 = activeKeys[i];
        const rem1 = currentReq[k1];
        if (rem1 <= lastAmount) candidateAmounts.add(rem1);
        const half = Math.floor(rem1 / 2);
        if (half > 0 && half <= lastAmount) candidateAmounts.add(half);
        const third = Math.floor(rem1 / 3);
        if (third > 0 && third <= lastAmount) candidateAmounts.add(third);

        for (let j = i + 1; j < activeKeys.length; j++) {
          const k2 = activeKeys[j];
          const rem2 = currentReq[k2];
          const diff = Math.abs(rem1 - rem2);
          if (diff > 0 && diff <= lastAmount) candidateAmounts.add(diff);
        }
      }

      const sortedAmounts = Array.from(candidateAmounts).sort((a, b) => b - a);
      const actions = [];

      for (const amt of sortedAmounts) {
        const singles = activeKeys.filter(k => currentReq[k] >= amt);
        const doubles = activeKeys.filter(k => currentReq[k] >= amt * 2);
        const triples = activeKeys.filter(k => currentReq[k] >= amt * 3);

        if (singles.length >= 3) {
          for (let i = 0; i < singles.length; i++) {
            for (let j = i + 1; j < singles.length; j++) {
              for (let l = j + 1; l < singles.length; l++) {
                const pat = [singles[i], singles[j], singles[l]];
                const pKey = pat.join(",");
                if (amt === lastAmount && pKey > lastPatternKey) continue;
                actions.push({ amt, pattern: pat, pKey });
              }
            }
          }
        }

        for (const k1 of doubles) {
          for (const k2 of singles) {
            if (k1 !== k2) {
              const pat = [k1, k1, k2];
              const pKey = pat.join(",");
              if (amt === lastAmount && pKey > lastPatternKey) continue;
              actions.push({ amt, pattern: pat, pKey });
            }
          }
        }

        for (const k of triples) {
          const pat = [k, k, k];
          const pKey = pat.join(",");
          if (amt === lastAmount && pKey > lastPatternKey) continue;
          actions.push({ amt, pattern: pat, pKey });
        }
      }

      actions.sort((a, b) => {
        const isMixedA = isPatternMixed(a.pattern) ? 1 : 0;
        const isMixedB = isPatternMixed(b.pattern) ? 1 : 0;
        return isMixedA - isMixedB;
      });

      return actions;
    }

    function isPatternMixed(pattern) {
      let hasBasic = false;
      let hasAdv = false;
      for (const it of pattern) {
        if (!it || it === "(無)") continue;
        if (it.startsWith("A") || it.startsWith("+")) hasAdv = true;
        else hasBasic = true;
      }
      return hasBasic && hasAdv;
    }

    function tryOneStepClear(currentReq, activeKeys, targetWaste) {
      const totalRem = activeKeys.reduce((sum, k) => sum + currentReq[k], 0);

      if (targetWaste === 0) {
        if (activeKeys.length === 1 && totalRem % 3 === 0) {
          return { lines: [activeKeys[0], activeKeys[0], activeKeys[0]], amount: totalRem / 3 };
        }
        if (activeKeys.length === 2) {
          const [k1, k2] = activeKeys;
          if (currentReq[k1] === currentReq[k2] * 2) return { lines: [k1, k1, k2], amount: currentReq[k2] };
          if (currentReq[k2] === currentReq[k1] * 2) return { lines: [k1, k2, k2], amount: currentReq[k1] };
        }
        if (activeKeys.length === 3) {
          const [k1, k2, k3] = activeKeys;
          if (currentReq[k1] === currentReq[k2] && currentReq[k1] === currentReq[k3]) {
            return { lines: [k1, k2, k3], amount: currentReq[k1] };
          }
        }
        return null;
      }

      if (targetWaste === 1) {
        if (totalRem !== 2) return null;
        if (activeKeys.length === 1) return { lines: [activeKeys[0], activeKeys[0], null], amount: 1 };
        if (activeKeys.length === 2 && currentReq[activeKeys[0]] === 1 && currentReq[activeKeys[1]] === 1) {
          return { lines: [activeKeys[0], activeKeys[1], null], amount: 1 };
        }
        return null;
      }

      if (targetWaste === 2) {
        if (totalRem !== 1) return null;
        if (activeKeys.length === 1 && currentReq[activeKeys[0]] === 1) {
          return { lines: [activeKeys[0], null, null], amount: 1 };
        }
        return null;
      }

      return null;
    }

    function solveGreedyFast(originalReq, targetWaste) {
      const req = { ...originalReq };
      const batches = [];

      for (const k of Object.keys(req)) {
        if (req[k] >= 3) {
          const times = Math.floor(req[k] / 3);
          batches.push({ lines: [k, k, k], amount: times });
          req[k] -= times * 3;
        }
      }

      function stepGreedy(keysSubset) {
        while (true) {
          const active = keysSubset.filter(k => req[k] > 0);
          if (active.length < 3) break;
          active.sort((a, b) => req[b] - req[a]);
          const amt = Math.min(req[active[0]], req[active[1]], req[active[2]]);
          if (amt <= 0) break;
          batches.push({ lines: [active[0], active[1], active[2]], amount: amt });
          req[active[0]] -= amt;
          req[active[1]] -= amt;
          req[active[2]] -= amt;
        }
      }

      stepGreedy(Object.keys(req).filter(k => !k.startsWith("A") && !k.startsWith("+")));
      stepGreedy(Object.keys(req).filter(k => k.startsWith("A") || k.startsWith("+")));

      while (true) {
        const active = Object.keys(req).filter(k => req[k] > 0);
        if (active.length === 0) break;

        const oneStep = tryOneStepClear(req, active, targetWaste);
        if (oneStep) {
          batches.push(oneStep);
          break;
        }

        active.sort((a, b) => req[b] - req[a]);

        if (active.length >= 3) {
          const amt = Math.min(req[active[0]], req[active[1]], req[active[2]]);
          batches.push({ lines: [active[0], active[1], active[2]], amount: amt });
          req[active[0]] -= amt;
          req[active[1]] -= amt;
          req[active[2]] -= amt;
        } else {
          return null;
        }
      }
      return batches;
    }

    function evaluateContinuity(rawBatches, getWeight, getOrderScore) {
      const copy = rawBatches.map(b => ({ lines: [...b.lines], amount: b.amount }));
      sortBatches(copy, getWeight, getOrderScore);
      clusterBatches(copy);
      return alignLinesOptimally(copy);
    }

    function sortBatches(batches, getWeight, getOrderScore) {
      batches.sort((a, b) => {
        const aHasNull = a.lines.includes(null);
        const bHasNull = b.lines.includes(null);
        if (aHasNull && !bHasNull) return 1;
        if (!aHasNull && bHasNull) return -1;
        const wA = a.lines.reduce((s, it) => s + getWeight(it), 0);
        const wB = b.lines.reduce((s, it) => s + getWeight(it), 0);
        if (wA !== wB) return wA - wB;
        if (a.amount !== b.amount) return a.amount - b.amount;
        for (let i = 0; i < 3; i++) {
          const sA = getOrderScore(a.lines[i]);
          const sB = getOrderScore(b.lines[i]);
          if (sA !== sB) return sA - sB;
        }
        return 0;
      });
    }

    function clusterBatches(batches) {
      for (let i = 0; i < batches.length - 1; i++) {
        const cur = batches[i];
        if (cur.lines.includes(null)) continue;
        const curItems = new Set(cur.lines.filter(Boolean));
        let bestIdx = -1, maxOverlap = 0;
        for (let j = i + 1; j < batches.length; j++) {
          const target = batches[j];
          if (target.lines.includes(null)) continue;
          let overlap = 0;
          for (const it of target.lines) if (it && curItems.has(it)) overlap++;
          if (overlap > maxOverlap) {
            maxOverlap = overlap;
            bestIdx = j;
          }
        }
        if (bestIdx > i + 1) {
          const [matched] = batches.splice(bestIdx, 1);
          batches.splice(i + 1, 0, matched);
        }
      }
    }

    function alignLinesOptimally(batches) {
      let totalMatches = 0;
      const perms = [[0,1,2],[0,2,1],[1,0,2],[1,2,0],[2,0,1],[2,1,0]];
      for (let i = 0; i < batches.length - 1; i++) {
        const cur = batches[i];
        const next = batches[i + 1];
        let bestP = perms[0], maxM = -1;
        for (const p of perms) {
          let m = 0;
          for (let l = 0; l < 3; l++) {
            const nextIt = next.lines[p[l]];
            if (cur.lines[l] && nextIt && cur.lines[l] === nextIt) m++;
          }
          if (m > maxM) { maxM = m; bestP = p; }
        }
        next.lines = [next.lines[bestP[0]], next.lines[bestP[1]], next.lines[bestP[2]]];
        totalMatches += maxM;
      }
      return totalMatches;
    }

    function formatResult(batches, getWeight, getOrderScore, materials, startTime, status, exploredNodes, message, targetWaste) {
      const copy = batches.map(b => ({ lines: [...b.lines], amount: b.amount }));
      sortBatches(copy, getWeight, getOrderScore);
      clusterBatches(copy);
      const continuousMatches = alignLinesOptimally(copy);
      const mixedBatches = countMixedBatches(copy);

      const displayAmounts = copy.map(b => [b.amount, b.amount, b.amount]);
      for (let i = 0; i < copy.length - 1; i++) {
        const cur = copy[i], next = copy[i + 1];
        for (let l = 0; l < 3; l++) {
          if (cur.lines[l] && next.lines[l] && cur.lines[l] === next.lines[l]) {
            displayAmounts[i][l] += next.amount;
          }
        }
      }

      let totalSlots = 0, idleSlots = 0;
      const productionList = copy.map((batch, i) => {
        const disp = displayAmounts[i];
        const nameStr = batch.lines.map(it => it || "(無)").join(" ");
        const validAmounts = [];
        for (let l = 0; l < 3; l++) {
          if (batch.lines[l] !== null) {
            validAmounts.push(disp[l]);
            totalSlots++;
          } else {
            idleSlots++;
          }
        }
        const isAllSame = validAmounts.every(a => a === validAmounts[0]);
        const title = isAllSame ? (nameStr + "：" + (validAmounts[0] ?? batch.amount)) : (nameStr + "：" + disp.join("+"));
        const inner = batch.lines.map(it => it ? { [it]: batch.amount } : null);
        return { [title]: inner };
      });

      const totalRequired = Object.values(materials).reduce((a, b) => a + b, 0);
      const totalOperations = totalSlots - continuousMatches;

      return {
        productionList: productionList,
        stats: {
          totalBatches: copy.length,
          totalOperations: totalOperations,
          continuousMatches: continuousMatches,
          mixedBatches: mixedBatches,
          idleSlots: targetWaste,
          totalProduced: totalRequired,
          totalRequired: totalRequired
        },
        debug: {
          status: status,
          isOptimal: status === "OPTIMAL",
          executionTimeMs: Math.round(performance.now() - startTime),
          exploredNodes: exploredNodes,
          message: message
        }
      };
    }
  `;

  // ==========================================
  // 主執行緒控制器 Class
  // ==========================================
  class ProductionScheduler {
    constructor() {
      this.worker = null;
      this.blobUrl = URL.createObjectURL(new Blob([workerCode], { type: "application/javascript" }));
      this.version = SCHEDULER_VERSION;
    }

    /**
     * 啟動/更新排程計算（自動中斷舊任務並優雅存檔）
     * @param {Object} materials - 需求物件，例如 {"角": 9, "錠": 10}
     * @param {Function} onMessage - 回調函式 ({ type, data }) => void
     */
    async calculate(materials, onMessage) {
      await this.stop();

      this.worker = new Worker(this.blobUrl);
      this.worker.onmessage = (e) => {
        const { type, data } = e.data;
        if (typeof onMessage === "function") {
          onMessage({ type, data });
        }
      };

      this.worker.postMessage({ type: "START", materials: materials });
    }

    /**
     * 優雅中斷背景計算（保存深層指針至 IndexedDB 後關閉）
     * @returns {Promise<void>}
     */
    stop() {
      return new Promise((resolve) => {
        if (!this.worker) {
          resolve();
          return;
        }

        const currentWorker = this.worker;
        this.worker = null;

        // 設定 80ms 超時保護，若 Worker 未及時回報則強制 terminate
        const forceKillTimer = setTimeout(() => {
          try { currentWorker.terminate(); } catch (e) {}
          resolve();
        }, 80);

        currentWorker.addEventListener("message", function onMsg(e) {
          if (e.data && e.data.type === "STOPPED") {
            clearTimeout(forceKillTimer);
            currentWorker.removeEventListener("message", onMsg);
            try { currentWorker.terminate(); } catch (e) {}
            resolve();
          }
        });

        try {
          currentWorker.postMessage({ type: "STOP" });
        } catch (e) {
          clearTimeout(forceKillTimer);
          try { currentWorker.terminate(); } catch (err) {}
          resolve();
        }
      });
    }
  }

  global.ProductionScheduler = ProductionScheduler;
})(typeof window !== "undefined" ? window : this);
