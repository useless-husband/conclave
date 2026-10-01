# conclave

**從零寫的 Raft 上建立的複寫式、線性一致的鍵值資料庫；用確定性模擬器在可重現的故障下執行真正的伺服器程式碼，並檢查每一次執行的線性一致性。**

[English](README.md) · [設計文件（英文）](docs/DESIGN.md) · [導讀（給初學者）](docs/導讀.zh-TW.md)

GitHub 上有幾百個 Raft 實作，大多用幾個寫好的情境來測，只能證明「沒出事時程式會動」。
conclave 的重點是證明「出事時也是對的」：

- **在模擬的災難下跑真正的程式碼。** 共識核心、請求處理路徑、預寫日誌（WAL）和狀態機自己不開
  goroutine、不讀時鐘、不做 I/O。`conclave` 執行檔用 TCP、硬碟和真實時鐘驅動它們；模擬器則在單一
  goroutine 裡驅動**同一份程式碼**：模擬網路（延遲、遺失、重複、亂序、對稱與單向分割）、模擬硬碟
  （當機只保留已 fsync 的位元組，最後一筆可能寫一半，當機可以發生在寫入、fsync 或 rename 的中途）、
  當機、暫停、成員變更和客戶端負載，全部由一個種子決定。同一個種子得到逐位元相同的執行。
- **每次執行都檢查。** 執行中檢查選舉安全性與狀態機安全性；所有故障修復後檢查活性；檢查各副本一致；
  並用本專案自己寫的檢查器驗證每個客戶端操作的線性一致性，包括客戶端不知道結果的寫入。
- **檢查是有牙齒的。** 十種「把真正程式碼的某一行改錯」的經典 Raft 錯誤只能從測試中開啟，
  模擬器在有限的種子數內全部抓到，下表列出各需要幾個種子。
- **真的找到過 bug。** 開發期間模擬器找到一個 WAL 復原的 bug：在不巧的時機當機後，兩次重開之後可能
  默默丟掉已確認的寫入（[細節](docs/DESIGN.md#4-the-write-ahead-log)）。它也揪出模擬客戶端本身的兩個建模錯誤
  （各造成一次誤報），所以下面突變表的每一次偵測，都會在拿掉 bug 後用同一個種子重跑，必須通過。

只用 Go 標準函式庫：約 9,000 行 Go 程式與 3,700 行測試。

## 試用

```console
$ scripts/cluster.sh start          # 建置並在 127.0.0.1 用隨機埠開 3 台，組成叢集
cluster running: n1 pid 78069, n2 pid 78099, n3 pid 78117
client addresses: 127.0.0.1:61697,127.0.0.1:61699,127.0.0.1:61701
$ . .cluster/env
$ .cluster/conclave put greeting hello
ok
$ .cluster/conclave get greeting
hello
$ .cluster/conclave cas greeting hello bonjour
ok
$ .cluster/conclave cas greeting hello hola
conclave: cas failed: current value is "bonjour"
$ .cluster/conclave delete greeting
deleted (was "bonjour")
$ scripts/cluster.sh kill 1         # 用 PID 送 SIGKILL 給領導者
killed n1 (pid 78069)
$ .cluster/conclave put survivor yes
ok
$ scripts/cluster.sh restart 1      # 同一個資料目錄、同一組埠
n1 running again, pid 78176
$ scripts/cluster.sh stop
```

HTTP API 和模擬器的完整範例請見英文 README。重播任一個種子：`go run ./cmd/conclave sim -seed 17 -trace`。

## 結果

### 每一種故意植入的 bug 都被抓到

`go test ./internal/sim -run TestMutationsAreDetected -v` 會開啟一種 bug，從種子 1、2、3…一直跑到模擬器
回報安全性違反為止，再用同一個種子在**沒有** bug 的情況下重跑一次，必須通過（排除是模擬器自己的問題）。
超過上限（約為下表數字的兩倍）測試就失敗。

| 植入的 bug（真正程式碼改錯一行） | 第一個失敗的種子 | 由什麼抓到 |
|---|---:|---|
| `vote-without-log-check`：投票時不檢查候選人日誌是否夠新（§5.4.1） | 2 | 狀態機安全性、線性一致性 |
| `commit-prior-term-by-count`：舊任期的條目在過半數機器上就宣布定案（Figure 8） | 3,397 | 狀態機安全性 |
| `vote-not-persisted`：投票前沒先把票寫進硬碟 | 282 | 選舉安全性（同一任期兩個領導者）、狀態機安全性、線性一致性 |
| `ack-before-fsync`：fsync 之前就確認收到條目 | 3 | 狀態機安全性 |
| `read-without-quorum`：領導者回答讀取前不做 ReadIndex 心跳確認 | 101 | 線性一致性（被取代的領導者讀到舊值） |
| `duplicate-apply`：忽略 session 表，重送的寫入再執行一次 | 1 | 線性一致性 |
| `skip-wal-checksum`：重放 WAL 時不檢查校驗碼 | 19 | 狀態機安全性（寫一半的紀錄被當成資料） |
| `skip-dir-sync`：建立 WAL 分段檔後不 fsync 目錄 | 5 | 狀態機安全性、選舉安全性、線性一致性 |
| `truncate-without-marker`：安裝領導者的快照時不寫作廢舊日誌的紀錄 | 1 | 復原失敗（WAL 打不開） |
| `conf-change-before-term-commit`：還沒在本任期提交任何條目就提出成員變更（Ongaro 2015） | 5,747 | 狀態機安全性、線性一致性 |

十種合計在 4 個 worker 上花 287 秒。最貴的兩種需要非常精準的當機順序；在模擬器學會「斷電時丟掉還沒送出的封包」和「新領導者第一次提交後立刻當機」之前，兩者在 2,000 個種子內都沒被找到。
成員變更的那個 bug 另外有 `TestOngaro2015MembershipBug` 一步一步重現。

### 未修改的程式碼

`go run ./cmd/conclave sim -seeds 1-40000 -workers 4`（最終版程式碼）：

```
seeds 1-40000: 40000 runs, 0 failed, 0 inconclusive checks
simulated 277.8 hours in 19m34s wall on 4 workers (852 simulated seconds per wall second)
4002243215 events, 3083148322 messages, 103160578 client operations checked for linearizability (616445 with unknown outcome)
1504264 crashes (702930 in the middle of disk I/O, 147135 torn writes), 238914 partitions, 122182 pauses, 953195 elections, 179997 snapshots installed, 39824 members added, 35465 removed
```

每個種子是 20 秒有故障加 5 秒無故障的虛擬時間。4 萬個種子等於模擬了 277.8 小時、1.03 億個客戶端操作，
每一個都經過檢查，沒有任何安全性或活性違反，也沒有檢查器跑不完的情況。模擬器每個 CPU 核心每秒約跑 210 秒模擬時間
（4 個 worker 合計 852；量測時機器同時在做其他工作）。

### 真實行程

`go test ./test/e2e -v` 會建置執行檔、開 3 台的叢集，用 6 個客戶端並行操作 20 秒，期間每 1 到 2.5 秒
用 PID 對某一台送 SIGKILL（一半機率是領導者）並在原資料目錄重開，最後用同一個檢查器檢查記錄下來的歷史。
把歷史中的一次讀取竄改後必須被判定違反，證明檢查不是空轉。

在這台機器上跑了三次（種子 1、2、3；每次 20 秒故障再 3 秒無故障）。一半的客戶端 300 毫秒沒回應就放棄，
所以在換領導者時會出現結果未知的寫入：

| 種子 | SIGKILL 次數 | 完成的操作 | 結果未知的寫入 | 判定 | 檢查耗時 |
|---|---|---|---|---|---|
| 1 | 8 | 7,616 | 6 | 線性一致 | 6 ms |
| 2 | 8 | 6,924 | 4 | 線性一致 | 6 ms |
| 3 | 8 | 7,498 | 4 | 線性一致 | 5 ms |

### 效能

`scripts/bench.sh` 為每種耐久性模式在 127.0.0.1 開一組新的 3 台叢集，用 `conclave bench` 測：
閉迴路客戶端（每個有自己的 session，收到回覆就送下一個），暖機 1 秒後量 10 秒，延遲在客戶端量測。
put 寫 64 位元組的值到 1,000 個 key；get 是線性一致的 ReadIndex 讀取；mixed 各半。

機器：Apple M5（10 核）、16 GB、內建 SSD、macOS 27、Go 1.27.1。伺服器和客戶端共用這台機器和同一顆 SSD，
量測時機器同時在做其他工作。

| 耐久性 | 工作負載 | 客戶端 | 每秒請求 | p50 | p99 |
|---|---|---:|---:|---:|---:|
| `full`（F_FULLFSYNC） | put | 1 | 99 | 10.98 ms | 13.94 ms |
| `full` | put | 16 | 532 | 30.87 ms | 38.99 ms |
| `full` | put | 128 | 4,166 | 31.22 ms | 42.13 ms |
| `full` | get | 1 | 11,192 | 0.09 ms | 0.15 ms |
| `full` | get | 128 | 103,946 | 1.09 ms | 3.46 ms |
| `none` | put | 1 | 6,921 | 0.13 ms | 0.30 ms |
| `none` | put | 16 | 28,405 | 0.56 ms | 1.17 ms |
| `none` | put | 128 | 55,669 | 2.15 ms | 5.04 ms |

耐久寫入受限於硬碟快取 flush：單獨一次 `F_FULLFSYNC` 約 3.7 毫秒，三台的 flush 又在同一顆 SSD 上排隊，所以單一客戶端約 11 毫秒；
客戶端多時批次提交讓一次 flush 涵蓋整批，吞吐從 99 提高到 4,166。讀取不碰硬碟，只需要一輪心跳。
`-fsync none` 不保證斷電安全，只用來看共識與 API 本身的成本。

## 運作方式

- **Raft**（`internal/raft`）：預投票、領導者黏著與 check-quorum 的選舉；管線化日誌複寫與快速衝突回退；
  只用當前任期的條目決定提交（Figure 8）；快照與 InstallSnapshot；一次一台的成員變更（含 2015 年的修正）；
  領導權交接；ReadIndex 讀取。`Flush` 一個函式定義耐久性規則：附加訊息可以在本地 fsync 之前送出，
  投票與確認只能在 fsync 之後。
- **預寫日誌**（`internal/wal`）：分段、每筆 CRC32C；復原時切掉最後一段被當機撕裂的寫入，其他地方損壞就拒絕啟動；
  macOS 用 `F_FULLFSYNC`。
- **狀態機**（`internal/kv`）：get、put、delete、compare-and-swap，以及讓重送的寫入只生效一次的 session。
- **模擬器**（`internal/sim`）：離散事件、單執行緒，所有決定來自一個種子，每個種子抽一組不同的故障組合；
  會在投票、當選等轉折點後刻意讓機器當機；一半的當機視為斷電，剛送出還在緩衝區的封包會一起消失。
- **檢查器**（`internal/lincheck`）：Wing–Gong–Lowe 搜尋加記憶化、按 key 分割，並對結果未知的操作做有證明的剪枝；
  與暴力列舉交叉驗證。

## 限制

- **只在一台機器上量測。** 三個行程共用一顆 SSD，硬碟快取的 flush 會互相排隊；分散在不同機器上的數字會不同。
  量測時這台機器同時在做其他工作。
- **模擬器的涵蓋是統計性的。** 它抓到了所有植入的 bug 和一個真的 bug，但需要它不會產生的故障序列、
  或需要比實際跑過更多種子的 bug 仍可能漏掉。最難的兩個植入 bug 需要上千個種子。
- **不處理拜占庭故障**：已同步資料的靜默損壞只會被校驗碼偵測並停止伺服器，不會從其他副本修復。
- **成員變更一次一台**（沒有 joint consensus），被移除的 ID 不可重用。
- **兩個埠都沒有認證或 TLS**，只能綁在可信任的網路。
- **只有領導者回答讀取**（ReadIndex），沒有 follower 讀取或租約。
- **狀態機在記憶體中**，大小受限於 RAM，快照是完整複本。
- 伺服器重開時必須回到加入時的位址。

## 相關專案

就我所知，同時附帶「對自己正式程式碼的確定性模擬器」、「獨立的線性一致性檢查器」和「實測的突變表」的開源
Raft 實作不多；但這些想法本身都有前例：FoundationDB 的模擬測試、TigerBeetle 的 VOPR、etcd/raft 與
hashicorp/raft、Jepsen/Knossos 與 Porcupine、MIT 6.5840 的 Raft 作業、Rust 的 MadSim 與 turmoil。
各自與 conclave 的差異請見英文 README 的 Related work。姊妹專案 linproof 是另外以 Lean 證明的檢查器，conclave 不依賴它。

## 建置與測試

需要 Go 1.23 以上，沒有其他相依套件。

```sh
make build        # bin/conclave
make short        # 單元測試、短的種子掃描、短的殺行程測試
make test         # 全部，包含突變表（數分鐘）
make sweep SEEDS=1-10000 WORKERS=4
make mutations    # 印出突變表
make e2e          # 真實行程的殺掉重開測試
make bench        # 效能表
make lint         # gofmt、go vet、staticcheck
```

## 授權

[MIT](LICENSE)
