/**
 * productionScheduler.js
 * 遊戲排程控制器（控制器守護存檔架構 v1.3.0）
 * 
 * 核心升級：
 * 1. 主執行緒單向防倒退門檻（One-Way Ratchet）：絕不覆蓋更優存檔。
 * 2. Worker 繼承歷史最佳門檻（Inherited Pruning Bound）：開局鎖死剪枝天花板，杜絕空間爆炸。
 * 3. 候選分支數量保護（Branch Capping）：限制單節點分支數，維持極限搜尋速率。
 * 4. 深度指針快進續傳（Fast-Forward Resumption）：真正實現斷點無縫接續。
 * 5. 版本升級至 1.3.0：啟動時自動清空舊版受污染快取。
 */
(function (global) {
  const SCHEDULER_VERSION = "1.3.0";
  const DB_NAME = "GameProductionSchedulerDB";
  const STORE_NAME = "checkpoints";

  // -------------------------------------------------------------
  // 主執行緒端 IndexedDB 儲存庫（穩定守護、自動版本遷移）
  // -------------------------------------------------------------
  let dbPromise = null;

  function getDB() {
    if (dbPromise) return dbPromise;
    dbPromise = new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, 4);
      req.onupgradeneeded = (e) => {
        const db = e.target.result;
        if (!db.objectStoreNames.contains(STORE_NAME)) {
          db.createObjectStore(STORE_NAME, { keyPath: "key" });
        }
      };
      req.onsuccess = async (e) => {
        const db = e.target.result;
        await checkVersionCleanup(db);
        resolve(db);
      };
      req.onerror = () => reject(req.error);
    });
    return dbPromise;
  }

  async function checkVersionCleanup(db) {
    try {
      const meta = await dbGet(db, "__META_VERSION__");
      if (!meta || meta.version !== SCHEDULER_VERSION) {
        await new Promise((resolve) => {
          const tx = db.transaction(STORE_NAME, "readwrite");
          tx.objectStore(STORE_NAME).clear();
          tx.oncomplete = () => resolve(true);
        });
        await dbPut(db, { key: "__META_VERSION__", version: SCHEDULER_VERSION });
      }
    } catch (e) {}
  }

  function dbGet(db, key) {
    return new Promise((resolve) => {
      try {
        const tx = db.transaction(STORE_NAME, "readonly");
        const req = tx.objectStore(STORE_NAME).get(key);
        req.onsuccess = () => resolve(req.result || null);
        req.onerror = () => resolve(null);
      } catch (e) {
        resolve(null);
      }
    });
  }

  function dbPut(db, record) {
    return new Promise((resolve) => {
      try {
        const tx = db.transaction(STORE_NAME, "readwrite");
        tx.objectStore(STORE_NAME).put(record);
        tx.oncomplete = () => resolve(true);
        tx.onerror = () => resolve(false);
      } catch (e) {
        resolve(false);
      }
    });
  }

  // -------------------------------------------------------------
  // 四級決策目標品質比較器（單向防倒退門檻核心）
  // -------------------------------------------------------------
  function compareSolutionQuality(solA, solB) {
    if (!solA && !solB) return 0;
    if (solA && !solB) return 1;
    if (!solA && solB) return -1;

    // 第 1 級：總批次數量少者優先
    if (solA.batchesLength !== solB.batchesLength) {
      return solB.batchesLength - solA.batchesLength;
    }
    // 第 2 級：玩家操作次數少者優先（同線連跑數多者優先）
    if (solA.continuityScore !== solB.continuityScore) {
      return solA.continuityScore - solB.continuityScore;
    }
    // 第 3 級：初、高階混合生產批次少者優先
    if (solA.mixedBatches !== solB.mixedBatches) {
      return solB.mixedBatches - solA.mixedBatches;
    }
    return 0;
  }

  // -------------------------------------------------------------
  // 抽象同構 Context 生成工具
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
    if (!abstractBatches) return [];
    return abstractBatches.map(b => ({
      lines: b.lines.map(id => id ? (idToName[id] || "(無)") : null),
      amount: b.amount
    }));
  }

  // ==========================================
  // Web Worker 核心運算代碼（純同步高效運算）
  // ==========================================
  const workerCode = `
    self.onmessage = function (e) {
      const { type, abstractReq, allowedWaste, resumeCursor, initialBest } = e.data;
      if (type === "START") {
        runCalculation(abstractReq, allowedWaste, resumeCursor, initialBest);
      }
    };

    function runCalculation(abstractReq, allowedWaste, resumeCursor, initialBest) {
      const startTime = performance.now();
      let lastProgressTime = startTime;
      const PROGRESS_INTERVAL_MS = 150;

      const inputKeys = Object.keys(abstractReq);
      const keyOrderMap = new Map(inputKeys.map((k, idx) => [k, idx]));
      const getWeight = (name) => (!name || name === "(無)") ? 0 : (name.startsWith("A") ? 1 : 0);
      const getOrderScore = (name) => (!name || name === "(無)") ? 999999 : (keyOrderMap.get(name) ?? 999999);

      let currentBestBatches = null;
      let minBatchesFound = Infinity;
      let maxContinuityScore = -1;
      let minMixedBatches = Infinity;
      let exploredNodes = 0;
      let currentCallStackPath = [];

      // 【核心規範 2】：若有歷史最佳存檔，直接繼承其門檻！開局第一微秒建立絕對剪枝鐵壁！
      if (initialBest && initialBest.abstractBatches) {
        currentBestBatches = initialBest.abstractBatches;
        minBatchesFound = initialBest.batchesLength;
        maxContinuityScore = initialBest.continuityScore;
        minMixedBatches = initialBest.mixedBatches;
      } else {
        // 僅在無任何存檔時執行快速初解
        const greedy = solveGreedyFast(abstractReq, allowedWaste);
        if (greedy && calculateWaste(greedy) === allowedWaste) {
          currentBestBatches = greedy;
          minBatchesFound = greedy.length;
          maxContinuityScore = evaluateContinuity(greedy, getWeight, getOrderScore);
          minMixedBatches = countMixedBatches(greedy);
          self.postMessage({
            type: "SOLUTION_UPDATE",
            abstractBatches: currentBestBatches,
            continuityScore: maxContinuityScore,
            mixedBatches: minMixedBatches,
            cursor: [],
            isOptimal: false,
            exploredNodes: 0
          });
        }
      }

      // 深層 DFS + 快進跳過
      let isFastForwarding = !!(resumeCursor && resumeCursor.length > 0);
      const req = { ...abstractReq };
      const topActions = getCandidateActions(req, Infinity, "");
      const totalTopActions = topActions.length;

      for (let topIdx = 0; topIdx < totalTopActions; topIdx++) {
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
        exploredNodes++;

        if ((exploredNodes & 4095) === 0) {
          const now = performance.now();
          if (now - lastProgressTime >= PROGRESS_INTERVAL_MS) {
            lastProgressTime = now;
            self.postMessage({
              type: "PROGRESS",
              data: {
                percent: Math.min(99, Math.max(0, branchProgress)),
                nodes: exploredNodes,
                elapsedMs: Math.round(now - startTime),
                currentBestBatches: minBatchesFound === Infinity ? 0 : minBatchesFound,
                currentBestOps: currentBestBatches ? (currentBestBatches.reduce((s, b) => s + b.lines.filter(Boolean).length, 0) - maxContinuityScore) : 0,
                currentMixedBatches: minMixedBatches === Infinity ? 0 : minMixedBatches,
                cursor: [...currentCallStackPath]
              }
            });
          }
        }

        // 剪枝鐵壁：批次數已達或超過已知最優，絕對秒剪
        if (currentBatches.length >= minBatchesFound) return;

        const activeKeys = Object.keys(currentReq).filter(k => currentReq[k] > 0);

        if (activeKeys.length === 0) {
          if (allowedWaste === 0) checkAndUpdateSolution(currentBatches);
          return;
        }

        const oneStep = tryOneStepClear(currentReq, activeKeys, allowedWaste);
        if (oneStep) {
          checkAndUpdateSolution([...currentBatches, oneStep]);
          return;
        }

        const actions = getCandidateActions(currentReq, lastAmount, lastPatternKey);

        for (let actIdx = 0; actIdx < actions.length; actIdx++) {
          if (isFastForwarding) {
            if (depth < resumeCursor.length) {
              const target = resumeCursor[depth];
              if (actIdx < target) continue;
              if (actIdx > target) isFastForwarding = false;
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

        // 四級字典序嚴格判定
        if (batches.length < minBatchesFound) {
          isBetter = true;
        } else if (batches.length === minBatchesFound) {
          if (continuity > maxContinuityScore) isBetter = true;
          else if (continuity === maxContinuityScore && mixed < minMixedBatches) isBetter = true;
        }

        if (isBetter) {
          minBatchesFound = batches.length;
          maxContinuityScore = continuity;
          minMixedBatches = mixed;
          currentBestBatches = batches.map(b => ({ lines: [...b.lines], amount: b.amount }));

          self.postMessage({
            type: "SOLUTION_UPDATE",
            abstractBatches: currentBestBatches,
            continuityScore: maxContinuityScore,
            mixedBatches: minMixedBatches,
            cursor: [...currentCallStackPath],
            isOptimal: false,
            exploredNodes
          });
        }
      }

      // 全域完成通知
      if (currentBestBatches) {
        self.postMessage({
          type: "COMPLETE",
          abstractBatches: currentBestBatches,
          continuityScore: maxContinuityScore,
          mixedBatches: minMixedBatches,
          cursor: null,
          isOptimal: true,
          exploredNodes
        });
      }
    }

    function calculateWaste(batches) {
      let waste = 0;
      for (const b of batches) {
        for (const it of b.lines) if (!it || it === "(無)") waste += b.amount;
      }
      return waste;
    }

    function countMixedBatches(batches) {
      let count = 0;
      for (const b of batches) {
        let hasB = false, hasA = false;
        for (const it of b.lines) {
          if (!it || it === "(無)") continue;
          if (it.startsWith("A")) hasA = true;
          else hasB = true;
        }
        if (hasB && hasA) count++;
      }
      return count;
    }

    // 【核心規範 4】：候選動作生成與分支數量保護（Branch Capping）
    function getCandidateActions(currentReq, lastAmount, lastPatternKey) {
      const activeKeys = Object.keys(currentReq).filter(k => currentReq[k] > 0);
      const candidateAmounts = new Set([1]);

      for (let i = 0; i < activeKeys.length; i++) {
        const r1 = currentReq[activeKeys[i]];
        if (r1 <= lastAmount) candidateAmounts.add(r1);
        const half = Math.floor(r1 / 2);
        if (half > 0 && half <= lastAmount) candidateAmounts.add(half);
        const third = Math.floor(r1 / 3);
        if (third > 0 && third <= lastAmount) candidateAmounts.add(third);

        for (let j = i + 1; j < activeKeys.length; j++) {
          const diff = Math.abs(r1 - currentReq[activeKeys[j]]);
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

      // 同階純組合排前，減少混搭
      actions.sort((a, b) => {
        let mixA = (a.pattern.some(x => x.startsWith("A")) && a.pattern.some(x => x.startsWith("B"))) ? 1 : 0;
        let mixB = (b.pattern.some(x => x.startsWith("A")) && b.pattern.some(x => x.startsWith("B"))) ? 1 : 0;
        return mixA - mixB;
      });

      // 截取前 36 個高潛力動作，杜絕組合爆炸
      return actions.slice(0, 36);
    }

    function tryOneStepClear(currentReq, activeKeys, targetWaste) {
      const totalRem = activeKeys.reduce((s, k) => s + currentReq[k], 0);

      if (targetWaste === 0) {
        if (activeKeys.length === 1 && totalRem % 3 === 0) return { lines: [activeKeys[0], activeKeys[0], activeKeys[0]], amount: totalRem / 3 };
        if (activeKeys.length === 2) {
          const [k1, k2] = activeKeys;
          if (currentReq[k1] === currentReq[k2] * 2) return { lines: [k1, k1, k2], amount: currentReq[k2] };
          if (currentReq[k2] === currentReq[k1] * 2) return { lines: [k1, k2, k2], amount: currentReq[k1] };
        }
        if (activeKeys.length === 3) {
          const [k1, k2, k3] = activeKeys;
          if (currentReq[k1] === currentReq[k2] && currentReq[k1] === currentReq[k3]) return { lines: [k1, k2, k3], amount: currentReq[k1] };
        }
        return null;
      }
      if (targetWaste === 1) {
        if (totalRem !== 2) return null;
        if (activeKeys.length === 1) return { lines: [activeKeys[0], activeKeys[0], null], amount: 1 };
        if (activeKeys.length === 2 && currentReq[activeKeys[0]] === 1 && currentReq[activeKeys[1]] === 1) return { lines: [activeKeys[0], activeKeys[1], null], amount: 1 };
      }
      if (targetWaste === 2) {
        if (totalRem !== 1) return null;
        if (activeKeys.length === 1 && currentReq[activeKeys[0]] === 1) return { lines: [activeKeys[0], null, null], amount: 1 };
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

      stepGreedy(Object.keys(req).filter(k => !k.startsWith("A")));
      stepGreedy(Object.keys(req).filter(k => k.startsWith("A")));

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
      copy.sort((a, b) => {
        const aHasNull = a.lines.includes(null), bHasNull = b.lines.includes(null);
        if (aHasNull && !bHasNull) return 1;
        if (!aHasNull && bHasNull) return -1;
        const wA = a.lines.reduce((s, it) => s + getWeight(it), 0);
        const wB = b.lines.reduce((s, it) => s + getWeight(it), 0);
        if (wA !== wB) return wA - wB;
        if (a.amount !== b.amount) return a.amount - b.amount;
        for (let i = 0; i < 3; i++) {
          const sA = getOrderScore(a.lines[i]), sB = getOrderScore(b.lines[i]);
          if (sA !== sB) return sA - sB;
        }
        return 0;
      });

      for (let i = 0; i < copy.length - 1; i++) {
        const cur = copy[i];
        if (cur.lines.includes(null)) continue;
        const curItems = new Set(cur.lines.filter(Boolean));
        let bestIdx = -1, maxOverlap = 0;
        for (let j = i + 1; j < copy.length; j++) {
          const target = copy[j];
          if (target.lines.includes(null)) continue;
          let overlap = 0;
          for (const it of target.lines) if (it && curItems.has(it)) overlap++;
          if (overlap > maxOverlap) { maxOverlap = overlap; bestIdx = j; }
        }
        if (bestIdx > i + 1) {
          const [matched] = copy.splice(bestIdx, 1);
          copy.splice(i + 1, 0, matched);
        }
      }

      let totalMatches = 0;
      const perms = [[0,1,2],[0,2,1],[1,0,2],[1,2,0],[2,0,1],[2,1,0]];
      for (let i = 0; i < copy.length - 1; i++) {
        const cur = copy[i], next = copy[i + 1];
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
  `;

  // -------------------------------------------------------------
  // 結果格式化工具（主執行緒端生成排程結構）
  // -------------------------------------------------------------
  function buildFinalOutput(concreteBatches, materials, startTime, status, exploredNodes, message, allowedWaste) {
    const keyOrderMap = new Map(Object.keys(materials).map((k, idx) => [k, idx]));
    const getWeight = (name) => (!name || name === "(無)") ? 0 : (name.startsWith("+") ? 1 : 0);
    const getOrderScore = (name) => (!name || name === "(無)") ? 999999 : (keyOrderMap.get(name) ?? 999999);

    const copy = concreteBatches.map(b => ({ lines: [...b.lines], amount: b.amount }));

    copy.sort((a, b) => {
      const aHasNull = a.lines.includes(null), bHasNull = b.lines.includes(null);
      if (aHasNull && !bHasNull) return 1;
      if (!aHasNull && bHasNull) return -1;
      const wA = a.lines.reduce((s, it) => s + getWeight(it), 0);
      const wB = b.lines.reduce((s, it) => s + getWeight(it), 0);
      if (wA !== wB) return wA - wB;
      if (a.amount !== b.amount) return a.amount - b.amount;
      for (let i = 0; i < 3; i++) {
        const sA = getOrderScore(a.lines[i]), sB = getOrderScore(b.lines[i]);
        if (sA !== sB) return sA - sB;
      }
      return 0;
    });

    for (let i = 0; i < copy.length - 1; i++) {
      const cur = copy[i];
      if (cur.lines.includes(null)) continue;
      const curItems = new Set(cur.lines.filter(Boolean));
      let bestIdx = -1, maxOverlap = 0;
      for (let j = i + 1; j < copy.length; j++) {
        const target = copy[j];
        if (target.lines.includes(null)) continue;
        let overlap = 0;
        for (const it of target.lines) if (it && curItems.has(it)) overlap++;
        if (overlap > maxOverlap) { maxOverlap = overlap; bestIdx = j; }
      }
      if (bestIdx > i + 1) {
        const [matched] = copy.splice(bestIdx, 1);
        copy.splice(i + 1, 0, matched);
      }
    }

    let continuousMatches = 0;
    const perms = [[0,1,2],[0,2,1],[1,0,2],[1,2,0],[2,0,1],[2,1,0]];
    for (let i = 0; i < copy.length - 1; i++) {
      const cur = copy[i], next = copy[i + 1];
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
      continuousMatches += maxM;
    }

    const displayAmounts = copy.map(b => [b.amount, b.amount, b.amount]);
    for (let i = 0; i < copy.length - 1; i++) {
      const cur = copy[i], next = copy[i + 1];
      for (let l = 0; l < 3; l++) {
        if (cur.lines[l] && next.lines[l] && cur.lines[l] === next.lines[l]) {
          displayAmounts[i][l] += next.amount;
        }
      }
    }

    let totalSlots = 0;
    const productionList = copy.map((batch, i) => {
      const disp = displayAmounts[i];
      const nameStr = batch.lines.map(it => it || "(無)").join(" ");
      const validAmounts = [];
      for (let l = 0; l < 3; l++) {
        if (batch.lines[l] !== null) {
          validAmounts.push(disp[l]);
          totalSlots++;
        }
      }
      const isAllSame = validAmounts.every(a => a === validAmounts[0]);
      const title = isAllSame ? (nameStr + "：" + (validAmounts[0] ?? batch.amount)) : (nameStr + "：" + disp.join("+"));
      const inner = batch.lines.map(it => it ? { [it]: batch.amount } : null);
      return { [title]: inner };
    });

    let mixedBatches = 0;
    for (const b of copy) {
      const hasB = b.lines.some(it => it && !it.startsWith("+") && it !== "(無)");
      const hasA = b.lines.some(it => it && it.startsWith("+"));
      if (hasB && hasA) mixedBatches++;
    }

    const totalRequired = Object.values(materials).reduce((a, b) => a + b, 0);
    const totalOperations = totalSlots - continuousMatches;

    return {
      productionList,
      stats: {
        totalBatches: copy.length,
        totalOperations,
        continuousMatches,
        mixedBatches,
        idleSlots: allowedWaste,
        totalProduced: totalRequired,
        totalRequired
      },
      debug: {
        status,
        isOptimal: status === "OPTIMAL",
        executionTimeMs: Math.round(performance.now() - startTime),
        exploredNodes,
        message
      }
    };
  }

  // ==========================================
  // 主執行緒控制器 Class（單向防倒退守護架構）
  // ==========================================
  class ProductionScheduler {
    constructor() {
      this.worker = null;
      this.blobUrl = URL.createObjectURL(new Blob([workerCode], { type: "application/javascript" }));
      this.currentTaskKey = null;
      this.latestCheckpoint = null;
      this.startTime = 0;
    }

    /**
     * 啟動排程計算（自動載入歷史存檔、秒回歷史最優、斷點續傳）
     */
    async calculate(materials, onMessage) {
      await this.stop();

      const startTime = performance.now();
      this.startTime = startTime;

      const { abstractReq, idToConcreteName, cacheKey } = buildIsomorphicContext(materials);
      this.currentTaskKey = cacheKey;
      this.latestCheckpoint = null;

      const totalRequired = Object.values(abstractReq).reduce((a, b) => a + b, 0);
      const allowedWaste = (3 - (totalRequired % 3)) % 3;

      const db = await getDB();
      const saved = await dbGet(db, cacheKey);

      let resumeCursor = null;
      let initialBest = null;

      // 1. 若有歷史存檔，0 毫秒秒出給前端
      if (saved && saved.key === cacheKey && saved.bestAbstractBatches) {
        this.latestCheckpoint = saved;
        resumeCursor = saved.cursor;
        initialBest = {
          batchesLength: saved.bestAbstractBatches.length,
          continuityScore: saved.continuityScore ?? 0,
          mixedBatches: saved.mixedBatches ?? 0,
          abstractBatches: saved.bestAbstractBatches
        };

        const concrete = restoreConcreteBatches(saved.bestAbstractBatches, idToConcreteName);
        const formatted = buildFinalOutput(concrete, materials, startTime, saved.isOptimal ? "OPTIMAL" : "SEARCHING", 0, saved.isOptimal ? "已確認為全域最優解（快取命中）！" : "已載入歷史最佳進度，深層斷點續傳中...", allowedWaste);

        if (typeof onMessage === "function") {
          onMessage({ type: "UPDATE", data: formatted });
        }

        // 若歷史已經證明最優，完全不需要啟動 Worker！
        if (saved.isOptimal) {
          if (typeof onMessage === "function") {
            onMessage({ type: "COMPLETE", data: formatted });
          }
          return;
        }
      }

      // 2. 啟動背景 Worker，傳入 initialBest 鎖死剪枝天花板
      this.worker = new Worker(this.blobUrl);

      this.worker.onmessage = async (e) => {
        const { type, data, abstractBatches, continuityScore, mixedBatches, cursor, isOptimal, exploredNodes } = e.data;

        if (this.currentTaskKey !== cacheKey) return;

        if (type === "PROGRESS") {
          if (this.latestCheckpoint && this.latestCheckpoint.key === this.currentTaskKey) {
            this.latestCheckpoint.cursor = data.cursor;
            this.latestCheckpoint.updatedAt = Date.now();
          }
          if (typeof onMessage === "function") {
            onMessage({ type: "PROGRESS", data });
          }
        } else if (type === "SOLUTION_UPDATE" || type === "COMPLETE") {
          // 【核心規範 1】：主執行緒單向防倒退檢驗（One-Way Ratchet）
          const candidateScore = {
            batchesLength: abstractBatches.length,
            continuityScore: continuityScore ?? 0,
            mixedBatches: mixedBatches ?? 0
          };

          let isStrictlyBetter = false;
          if (!this.latestCheckpoint || !this.latestCheckpoint.bestAbstractBatches) {
            isStrictlyBetter = true;
          } else {
            const currentScore = {
              batchesLength: this.latestCheckpoint.bestAbstractBatches.length,
              continuityScore: this.latestCheckpoint.continuityScore ?? 0,
              mixedBatches: this.latestCheckpoint.mixedBatches ?? 0
            };
            isStrictlyBetter = compareSolutionQuality(candidateScore, currentScore) > 0;
          }

          // 只有嚴格優於現有存檔時，才允許更新！劣質解一律拋棄，嚴禁覆蓋！
          if (isStrictlyBetter) {
            this.latestCheckpoint = {
              key: cacheKey,
              version: SCHEDULER_VERSION,
              bestAbstractBatches: abstractBatches,
              continuityScore: candidateScore.continuityScore,
              mixedBatches: candidateScore.mixedBatches,
              cursor: isOptimal ? null : cursor,
              isOptimal: !!isOptimal,
              updatedAt: Date.now()
            };

            dbPut(db, this.latestCheckpoint);

            const concrete = restoreConcreteBatches(abstractBatches, idToConcreteName);
            const formatted = buildFinalOutput(concrete, materials, this.startTime, isOptimal ? "OPTIMAL" : "SEARCHING", exploredNodes, isOptimal ? "已確認為全域最優解！" : "找到更優排程：操作次數已降低！", allowedWaste);

            if (typeof onMessage === "function") {
              onMessage({ type: isOptimal ? "COMPLETE" : "UPDATE", data: formatted });
            }
          } else if (isOptimal && this.latestCheckpoint) {
            // 已全空間探索完畢，標記最優解
            this.latestCheckpoint.isOptimal = true;
            this.latestCheckpoint.cursor = null;
            dbPut(db, this.latestCheckpoint);

            const concrete = restoreConcreteBatches(this.latestCheckpoint.bestAbstractBatches, idToConcreteName);
            const formatted = buildFinalOutput(concrete, materials, this.startTime, "OPTIMAL", exploredNodes, "已確認為全域最優解！", allowedWaste);

            if (typeof onMessage === "function") {
              onMessage({ type: "COMPLETE", data: formatted });
            }
          }
        }
      };

      this.worker.postMessage({
        type: "START",
        abstractReq,
        allowedWaste,
        resumeCursor,
        initialBest // 【核心】：傳入歷史最佳門檻
      });
    }

    /**
     * 終止計算（確保當前最新斷點 100% 寫入 IndexedDB）
     */
    async stop() {
      if (this.worker) {
        try { this.worker.terminate(); } catch (e) {}
        this.worker = null;
      }

      if (this.latestCheckpoint && this.currentTaskKey && this.latestCheckpoint.key === this.currentTaskKey) {
        try {
          const db = await getDB();
          await dbPut(db, this.latestCheckpoint);
        } catch (e) {}
      }

      this.currentTaskKey = null;
      this.latestCheckpoint = null;
    }
  }

  global.ProductionScheduler = ProductionScheduler;
})(typeof window !== "undefined" ? window : this);
