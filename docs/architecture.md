# moraebot 전체 구조

> 상태: 설계 초안 v2, 2026-11-15, 구현 전.
> 이 문서는 설계만 적습니다. 아직 코드는 없습니다.
> "미검증"이라고 적은 부분은 조사에서 직접 확인하지 못한 내용입니다.

## 한 장 요약

moraebot은 Discord로 받은 메시지를 이 컴퓨터의 코딩 에이전트 omp에게 넘기고, omp의 답을 다시 Discord로 보내는 프로그램입니다.
처음 나오는 말은 바로 아래 "용어 풀이"에서 풀어 둡니다.

```
 Discord (주인의 메시지)
     |
     |  Gateway WebSocket (계속 연결된 통신)
     v
 moraebot 게이트웨이 (Rust, 항상 켜져 있음)
     |  1. 접근 제어 검사
     |  2. SQLite에 먼저 저장 (메시지 + 중복 방지 키)
     v
 SQLite (inbox.sqlite)
     |
     |  전체 대기열 하나 (먼저 온 순서)
     v
 omp --mode rpc --session-dir <홈>/lead/session   (모든 대화에 lead omp 하나)
     |  긴 일은 lane_start로 lane omp에게 (최대 3개)
     |
     |  prompt_result 도착
     v
 Discord에 답장
```

## 용어 풀이

이 문서에 자주 나오는 말을 먼저 풀어 둡니다.

- **Rust**: 프로그램을 만드는 언어 하나입니다. 게이트웨이를 이 언어로 만듭니다.
- **SQLite**: 파일 하나로 된 작은 데이터베이스입니다. 받은 메시지를 여기에 저장합니다.
- **게이트웨이**: Discord와 omp 사이에서 메시지를 받고 넘기는 moraebot의 본체 프로그램입니다.
- **프로세스**: 컴퓨터에서 실행 중인 프로그램 하나입니다. "자식 프로세스"는 게이트웨이가 직접 실행하고 관리하는 프로그램입니다.
- **RPC**: 다른 프로그램에 명령을 보내고 결과를 돌려받는 방식입니다. 여기서는 게이트웨이가 omp에 명령을 보내는 방식을 말합니다.
- **큐**: 들어온 순서대로 하나씩 꺼내 처리하는 대기 줄입니다.
- **트랜잭션**: 여러 저장 작업을 "전부 되거나, 하나도 안 되거나"로 묶는 단위입니다.
- **중복 방지 키**: 같은 메시지를 두 번 저장하지 않도록 메시지마다 붙이는 고유 값입니다. 여기서는 Discord 메시지 id를 씁니다.
- **토큰**: 봇이 Discord에 로그인할 때 쓰는 비밀 문자열입니다. 남이 알면 봇을 가져갈 수 있습니다.
- **DM**: Discord의 1:1 개인 메시지입니다.
- **id**: 사용자, 채널, 메시지마다 Discord가 붙이는 고유 번호입니다.
- **Discord 스레드**: Discord 채널 안에서 갈라져 나온 작은 대화방입니다. 이 문서에서 "스레드"만 쓰면 이것을 뜻합니다.
- **별도 작업 흐름(스레드)**: 프로그램 안에서 다른 일과 따로 도는 작업 단위입니다. Discord 스레드와는 다른 말입니다. 3장과 7장에만 나옵니다.
- **launchd / LaunchAgent**: launchd는 macOS가 프로그램을 자동으로 켜고 다시 켜 주는 기능입니다. LaunchAgent는 사용자가 로그인했을 때 그 사용자 권한으로 도는 launchd 등록입니다.
- **systemd / 사용자 서비스**: systemd는 Linux가 프로그램을 자동으로 켜고 다시 켜 주는 기능입니다. 사용자 서비스(`systemctl --user`)는 그 사용자 권한으로 도는 systemd 등록이고, 맥의 LaunchAgent와 짝입니다.
- **커밋**: git(코드 버전 관리 도구)으로 관리하는 코드의 특정 저장 시점입니다. 커밋으로 고정하면 늘 같은 버전의 코드를 씁니다.
- **hermes**: 조사한 다른 오픈소스 메신저 봇 프로젝트(hermes-agent)입니다. Discord 연결과 접근 제어를 참고합니다.
- **dinobot**: 조사한 또 다른 메신저 봇 프로젝트입니다. "먼저 저장하고 처리" 원칙을 참고합니다. 비상업용 라이선스라 코드는 가져오지 않습니다.
- **omp**: 이 컴퓨터에서 실제로 코딩 일을 하는 에이전트 프로그램(oh-my-pi)입니다.
- **Message Content intent**: 봇이 서버 메시지의 본문을 읽으려면 Discord 개발자 포털에서 켜야 하는 권한 설정입니다.
- **prompt_result**: omp가 메시지 하나에 대한 일을 마쳤을 때 보내는 "끝났다" 신호입니다. 성공, 중단, 오류 중 어느 쪽인지 들어 있습니다.

## 확정된 결정

아래 결정은 이미 정해졌습니다. 이 문서는 이 결정을 바꾸지 않습니다.

| 번호 | 결정 | 쉬운 설명 |
|---|---|---|
| D1 | 일하는 에이전트는 omp | omp는 oh-my-pi(github.com/can1357/oh-my-pi)입니다. 설치된 버전은 18.20.2입니다. |
| D2 | omp는 yolo 모드 | omp가 도구를 쓸 때 묻지 않고 바로 실행합니다. |
| D3 | 첫 메신저는 Discord | Gateway WebSocket으로 일반 메시지를 받습니다. 이를 위해 Message Content intent를 켜야 합니다. Telegram, Slack은 나중입니다. |
| D4 | 게이트웨이는 Rust로 만든 별도 프로그램 | 항상 켜져 있는 프로그램 하나입니다. |
| D5 | 접근 제어는 바꿀 수 있는 설정 | `owner_only`와 `allowlist` 중 하나를 고릅니다. 기본값은 `owner_only`입니다. `allowlist`는 메신저 섹션에 적어 둔 사용자와 봇만 추가로 허용합니다(2.6절). `pairing`과 `open`은 지금 계획에 없습니다. |
| D6 | 먼저 저장하고, 그다음 처리 | 받은 메시지를 SQLite에 먼저 저장하고 처리합니다. dinobot의 원칙만 참고하고 코드는 새로 씁니다. |
| D7 | 모든 대화에 lead omp 하나 (slice 4) | `omp --mode rpc --session-dir <홈>/lead/session` 하나가 모든 대화를 맡습니다. 그 메시지의 `prompt_result`가 오면 메시지가 온 대화로 답장합니다. 처음 설계(대화마다 omp 하나)는 slice 4에서 바뀌었습니다(3.7절). |
| D8 | 작게 시작해 하나씩 | 한 번에 기능 하나만 만듭니다. 지금은 문서만 씁니다. |
| D9 | 쉬운 한국어 | 주인은 개발자가 아닐 수 있습니다. 어려운 말은 처음 나올 때 풀어 씁니다. |

**가장 중요한 위험.** omp가 yolo 모드라서, 봇과 말할 수 있는 사람은 사실상 이 컴퓨터에서 아무 명령이나 실행할 수 있습니다. 파일 삭제, 비밀번호 파일 읽기, 외부로 데이터 보내기도 됩니다. 접근을 허락하는 것은 이 컴퓨터를 맡기는 것과 같은 결정입니다. 이 위험은 2장에서 다시 설명합니다.

## 1. 전체 구조

moraebot은 세 부분으로 이루어집니다.

1. **게이트웨이**: Rust로 만든 프로그램 하나입니다. Discord와 계속 연결되어 메시지를 받고, 저장하고, omp에 넘기고, 답장을 보냅니다.
2. **SQLite**: 파일 하나로 된 작은 데이터베이스입니다. 별도 서버가 필요 없습니다. 받은 메시지, 대화 목록, 허용 사용자 같은 상태를 저장합니다.
3. **omp 자식 프로세스들**: 자식 프로세스는 게이트웨이가 직접 실행하고 관리하는 프로그램입니다. 모든 대화를 맡는 lead omp 하나와, lead가 긴 일을 맡기는 lane omp가 최대 3개 돕니다(3.7절).

> **slice 2 메모.** 이제 Slack도 붙습니다. Slack은 Socket Mode(WebSocket)로 연결합니다. #40부터 Slack 연결은 코어 밖의 별도 프로세스 `moraebot-slack-bridge`(코드는 `src/slack_bridge/`)가 맡고, 코어는 로컬 unix socket으로 브리지와 이야기합니다(`messenger = "bridge"`, 형식은 `docs/bridge.md`). Discord는 지금도 코어 안 어댑터입니다. 메신저는 한 번에 하나만 켜집니다(`config.toml`의 `messenger`). 앱 설정 방법은 `docs/guides/`의 메신저별 안내서에 있습니다. 아래 그림의 Discord 자리에 Slack이 들어가도 흐름은 같습니다.

메시지 하나가 지나가는 길은 이렇습니다.

```
 Discord Gateway (WebSocket)
          |
          v
 [1] 접근 제어 검사 ---- 거부 --> 무시
          |
          v
 [2] 제어 명령? (/stop, /new) --예--> 바로 실행 (줄 서지 않음)
          | 아니오
          v
 [3] SQLite 트랜잭션: 메시지 + 대화 키 + 중복 방지 키  (상태 = queued)
          |
          v
 [4] 대화 키별 큐
     th:111 [m1][m2]   th:222 [m3]   dm:333 [m4]
        |                  |              |
        v                  v              v
     omp #1             omp #2         omp #3   (서로 기다리지 않음)
          |
          v
 [5] 접근 제어 다시 검사 -> prompt -> prompt_result -> Discord 답장
```

## 2. 메시지 받기와 접근 제어

### 2.1 Discord에 붙는 방법

게이트웨이는 Discord Gateway에 WebSocket으로 늘 연결해 둡니다. WebSocket은 한 번 연결하면 계속 메시지를 주고받는 통신 방식입니다. hermes도 이 방식으로 일반 메시지를 받습니다.

- **intent**: 봇이 어떤 이벤트를 받을지 Discord에 미리 알리는 설정입니다. 서버 메시지와 DM 메시지 intent가 필요합니다.
- **Message Content intent**: 메시지 본문을 읽으려면 이 특권(privileged) intent를 켜야 합니다. 주인이 Discord 개발자 포털에서 직접 켭니다. 꺼져 있으면 일반 서버 메시지의 본문이 비어서 올 수 있습니다. DM과 봇을 멘션한 메시지는 예외일 수 있습니다. 이 동작은 조사 자료에 근거가 없어 **미검증**입니다.
- **재연결과 이어받기(resume)**: 연결이 끊기면 다시 붙고, 가능하면 끊긴 곳부터 이어받습니다. Discord 라이브러리가 이 일을 해 준다고 조사 자료에 적혀 있지만, 어디까지 해 주는지는 **미검증**입니다.
- **놓치는 메시지**: Discord는 오래 끊긴 동안의 메시지를 나중에 다시 보내 주지 않습니다. 게이트웨이가 꺼져 있던 동안 온 메시지는 놓칠 수 있습니다(열린 질문 5).

dinobot은 Discord를 interactions 웹훅으로만 받습니다. 이 방식은 슬래시 명령만 받고 일반 메시지는 못 받습니다. moraebot은 이 방식을 쓰지 않습니다.

**Rust 라이브러리.** Discord 쪽 후보는 `serenity` 0.12.5와 `twilight-gateway` 0.18.0입니다. serenity는 기능이 다 들어 있어 쓰기 쉽고, twilight는 필요한 부분만 골라 쓰는 가벼운 구성입니다. **serenity로 시작**하기를 권장합니다. 첫 단계는 받아서 저장하고 넘기는 일뿐이라 쉬운 쪽이 낫습니다. SQLite는 Rust용 라이브러리 `rusqlite` 0.43.0을 씁니다. 쓰기는 한 곳(쓰기만 맡는 별도 작업 흐름(스레드) 하나)에서만 합니다.

**Slack은 이렇게 붙습니다(slice 2, #40에서 브리지로 이동).** 브리지 `moraebot-slack-bridge`가 Socket Mode로 연결합니다. 공개 HTTP 주소가 필요 없습니다. Slack 토큰과 `channel_id`는 브리지 설정(`<home>/slack-bridge.toml`)에만 있고, 접근 판단은 그대로 코어가 합니다. 고정 설정(scope, event, agent view)은 `docs/guides/slack.md`에만 적습니다. 알아 둘 점 두 가지입니다.

- 브리지는 Slack 이벤트를 받자마자 ack하고 디스크의 spool에 먼저 씁니다. 코어가 `handle_event`로 행을 저장한 뒤 ack해야 spool에서 지웁니다. 그래서 코어가 꺼져 있거나 ack 전에 죽어도 메시지는 사라지지 않고, 다시 넘어온 메시지는 메시지 id(`<channel>:<ts>`)로 한 번만 저장됩니다. Discord에는 이 보호가 없습니다.
- Slack의 `file_share` 메시지는 slice 3부터 일반 메시지처럼 받습니다. 첨부 파일 정보가 함께 들어오고, 아래 파일 흐름을 탑니다.

**파일 흐름(slice 3).** 받는 쪽과 보내는 쪽이 따로 있습니다. 코드는 `src/files.rs`(메신저와 무관한 부분)와 `src/dispatch.rs`에 있습니다.

- **받기**: 주인 확인 → `admit`(저장 먼저) → 스레드 열기 → `fetch_attachment`로 내려받기 → `<work_dir>/.moraebot-inbox/<대화>/<메시지>/`에 저장 → 프롬프트 앞에 저장 경로 목록(prompt block)을 붙여 omp에 넘깁니다. 파일이 다 저장될 때까지 그 메시지는 대기열(FIFO)에 들어가지 않습니다.
  - 첨부가 `MAX_FILES`(10)개보다 많으면 저장도 내려받기도 하지 않고 `N_FILE_NOT_RECEIVED`를 보냅니다(`E_FILE_TOO_MANY`).
  - 내려받기는 `STALL_TIMEOUT`(2분) 동안 데이터가 없을 때만 실패합니다(`E_FILE_FETCH`). 허용되지 않은 호스트는 `E_FILE_HOST`, 메신저가 없다고 하면 `E_FILE_REMOTE`, 디스크 쓰기 실패는 `E_FILE_WRITE`입니다. 하나라도 실패하면 메시지는 `failed`(`E_FILE_INCOMPLETE`)가 되고 `N_FILE_NOT_RECEIVED` 알림이 한 번 갑니다.
  - 권한이 없어 거절되면 `E_FILE_DENIED`이고 알림은 `N_FILE_NOT_ALLOWED`입니다.
- **보내기**: omp가 호스트 도구 `send_file`을 부름 → 경로 검사(`E_FILE_PATH`: 작업 폴더 밖, moraebot 홈 안, 일반 파일 아님) → 답과 함께 `out_files`에 저장(`reply_pending`) → 답 텍스트를 보낸 뒤 파일마다 업로드합니다. 파일마다 보냄/건너뜀을 따로 기록해서 다시 보낼 때는 빠진 것만 보냅니다.
  - 업로드 한 번이 `UPLOAD_TIMEOUT`(30분)을 넘으면 `E_FILE_UPLOAD_TIMEOUT`으로 건너뛰고 다시 시도하지 않습니다. 메신저가 거절하면(`E_ADAPTER_UPLOAD_DENIED`) 역시 건너뜁니다.
  - 그 밖의 실패(`E_ADAPTER_UPLOAD`)는 재연결 때 다시 시도하며, 재시작을 포함해 파일당 `MAX_UPLOAD_ATTEMPTS`(3)번까지입니다. 업로드 도중 멈추거나 죽은 파일은 두 번 보내지 않도록 건너뜁니다.
  - 건너뛴 파일이 있으면 답 하나당 `N_FILE_NOT_SENT` 알림을 한 번 보냅니다.
- **경로 검사의 한계**: 보낼 경로는 도구를 부를 때와 업로드 직전에 두 번 검사하지만, 업로드할 때는 경로로 파일을 다시 엽니다. 그 사이에 파일이 바뀌어 들어갈 수 있습니다. 에이전트가 이미 파일 전체에 접근할 수 있으므로 받아들인 한계이고, 단단한 보안 경계가 아닙니다.

### 2.2 무엇을 "대화 하나"로 볼까

slice 4부터 모든 대화는 lead omp 하나가 맡습니다. 그래도 답장을 메시지가 온 대화로 보내야 하므로 메시지마다 **대화 키**를 정합니다. 대화 키는 이 메시지가 어느 대화에 속하는지 나타내는 문자열입니다.

| 메시지가 온 곳 | 대화 키 | 비고 |
|---|---|---|
| DM | `discord:dm:<채널id>` | 1:1이라 단순합니다 |
| 서버 채널 | `discord:ch:<채널id>` | 채널 하나가 대화 하나 |
| Discord 스레드 | `discord:th:<스레드id>` | **권장 기본 단위** |
| Slack DM 스레드 | `slack:th:<채널id>:<스레드 ts>` | 브리지가 DM마다 `thread_id`를 채움. 루트 DM은 자기 `ts`가 스레드 |

Slack DM은 브리지(agent mode)부터 스레드 하나가 대화 하나입니다. 루트 DM에는 그 메시지 `ts`로 스레드 답장을 하고, 스레드 안 DM은 같은 스레드에 답합니다. 예전 `slack:dm:<채널id>` 대화는 남은 답장만 그대로 보냅니다. 코어는 `thread_id`를 해석하지 않는 문자열로 다룹니다.

**권장: Discord 스레드 하나가 대화 하나입니다.** 작업마다 스레드를 열면 대화가 깔끔하게 나뉘고, 여러 작업을 동시에 돌리기도 쉽습니다. hermes도 플랫폼, 채팅, 스레드, 사용자를 이어 붙여 키를 만듭니다. 대화 폴더 이름은 대화 키에서 `:`를 `-`로 바꿔 만듭니다(예: `discord-th-111`).

### 2.3 먼저 저장하고, 그다음 처리

메시지를 받으면 **처리하기 전에 SQLite에 먼저 저장**합니다. 이 원칙은 dinobot에서 가져왔고, 코드는 새로 씁니다. dinobot 라이선스는 비상업용이라 참고만 합니다.

- 한 트랜잭션 안에서 메시지 본문, 대화 키, 중복 방지 키(Discord 메시지 id)를 함께 넣습니다. 트랜잭션은 "전부 저장되거나, 하나도 저장되지 않거나" 둘 중 하나만 일어나게 하는 묶음입니다. 나중에 받은 위치 표시(커서)를 쓰는 메신저가 생기면 그 값도 같은 트랜잭션에 넣습니다.
- 중복 방지 키에는 UNIQUE 제약(같은 값이 두 번 들어갈 수 없다는 규칙)을 겁니다. 같은 메시지가 두 번 오면 두 번째는 저장되지 않고 건너뜁니다.
- 저장이 끝난 뒤에만 처리를 시작합니다.

dinobot은 상태 6개(`queued`, `processing`, `ready`, `sending`, `done`, `dead`)를 씁니다. moraebot은 4개로 시작합니다. 메시지 상태는 `queued`(대기), `processing`(처리 중), `done`(완료)로 바뀌고, 실패가 반복되면 `dead`가 됩니다. omp가 이상하게 끝나서 결과를 믿기 어려우면 "확인 필요"로 남깁니다(3장).

### 2.4 순서: 대화마다 큐 하나

큐는 들어온 순서대로 하나씩 꺼내 처리하는 줄입니다. moraebot은 **대화마다 큐를 하나씩** 둡니다.

- 같은 대화 안에서는 순서를 지킵니다. 앞 메시지가 끝나야 다음 메시지가 갑니다.
- 다른 대화끼리는 서로 기다리지 않습니다.

dinobot의 Discord와 Slack은 전체에 큐 하나(동시 처리 1개)를 씁니다. 그러면 한 Discord 스레드에서 긴 코딩 작업이 돌 때 다른 Discord 스레드가 전부 멈춰 기다립니다. 이것을 head-of-line blocking(맨 앞 작업이 뒤를 전부 막는 현상)이라고 합니다. 코딩 작업은 길어서 이 문제가 크게 드러나므로 따라 하지 않습니다.

처리 중인 대화에 새 메시지가 오면, 기본으로는 지금 일이 끝난 뒤에 이어서 처리합니다(3.4절, 열린 질문 4).

### 2.5 제어 명령은 줄 서지 않음

`/stop`(지금 작업 멈추기)과 `/new`(새 대화 시작)는 큐에 넣지 않고 **바로 실행**합니다. hermes가 이렇게 합니다. 큐에 넣으면 작업이 끝날 때까지 `/stop`이 기다려야 하므로 멈출 수가 없습니다.

- `/stop`: 그 대화의 omp에 `abort`를 보냅니다.
- `/new`: 지금 대화를 보관하고, 같은 키에 새 세션 폴더를 연결합니다. dinobot도 옛 대화를 `<키>#n`으로 보관합니다. (Implemented in #74 as the owner's new-session command for the one lead; it waits for the running turn instead of skipping the queue. See 3.19.)

제어 명령도 접근 제어는 똑같이 거칩니다.

### 2.6 접근 제어: 누가 봇과 말할 수 있나

접근 제어는 코드에 고정하지 않고 **주인이 `config.toml`에서 바꾸는 설정**입니다. 채팅으로는 바꿀 수 없습니다. 모드는 두 가지입니다. 기본 거부(명단에 없으면 막기)는 hermes에서 가져온 원칙입니다.

| 모드 | 뜻 | 위험도 |
|---|---|---|
| `owner_only` (기본값) | 주인 한 명만 | 가장 낮음 |
| `allowlist` | 주인과, 메신저 섹션의 `allow` 목록에 적은 사용자와 봇 | 목록 관리에 달림 |

처음 설계에 있던 `pairing`(모르는 사람이 코드를 받고 주인이 승인)과 `open`(누구나)은 지금 계획에 없습니다. 필요해지면 따로 설계합니다.

허용 목록은 메신저마다 따로 적습니다. id 모양이 메신저마다 달라서(Discord는 숫자, Slack은 `U...`, Slack 봇은 `B...` bot id도 됨), 섹션 안에 두어야 기존 id 검사를 그대로 쓸 수 있습니다.

```toml
access_mode = "allowlist"

[bridge]
socket = "<소켓 경로>"       # 브리지 설정과 같은 절대 경로
owner_id = "U0OWNER"        # 주인은 그대로 owner_id
max_bot_replies = 30        # 선택. 스레드 하나에서 10분 동안 봇에게 답하는 최대 횟수. 없으면 제한 없음

[[bridge.allow]]
id = "U0FRIEND"
kind = "user"               # "user" 또는 "bot"
role = "chat"               # 지금은 "chat"만 있음. 빠지면 "chat"
```

설정을 읽을 때 엄격하게 검사합니다. 애매하면 시작하지 않습니다(`E_CONFIG_VALUE`).

- `allow`나 `max_bot_replies`가 있는데 모드가 `owner_only`면 오류입니다. 목록이 켜진 것처럼 보이지 않게 하려는 것입니다.
- 모르는 `kind`나 `role`, 빠진 `id`나 `kind`, 같은 id 두 번, 주인 id, 메신저 모양에 맞지 않는 id는 오류입니다.
- `max_bot_replies = 0`은 오류입니다. 제한을 없애려면 줄을 지웁니다.
- 예전 moraebot은 이 키를 모르므로 새 설정을 거부합니다.
- `moraebot check-config`는 허용된 id마다 kind와 role을 `WARN` 줄로 보여 줍니다.

역할은 둘입니다.

- **주인(owner)**: 지금과 같습니다. lead omp, lane, 모든 관리 도구를 씁니다.
- **chat**: 대화만 합니다. 답은 lead가 아니라 도구가 하나도 없는 별도 omp가 대화마다 따로 세션을 두고 만듭니다. lane을 멈추거나 닫는 관리 도구는 주인만 씁니다. 파일은 보낼 수 있습니다.

일을 시키는 `work` 역할은 v1에 없습니다. lead가 yolo로 돌기 때문에 work는 사실상 이 컴퓨터의 셸을 주는 것과 같고, 봇에게는 앞으로도 주지 않습니다.

봇끼리 끝없이 주고받지 않도록 두 겹으로 막습니다. `max_bot_replies`가 있으면 그 수가 강제 상한이고, 없으면 상한이 없습니다. 그와 별도로 lead 안내문에 "봇과 끝없이 주고받지 말라"는 한 줄을 넣습니다. 이것은 지침일 뿐 강제가 아닙니다.

> **chat용 omp (`src/chat.rs`).** chat 메시지 하나마다 omp를 새로 띄우고 답을 받은 뒤 닫습니다. 띄울 때 `--no-tools --no-extensions --no-skills --no-rules --no-context-files --no-lsp --no-pty --no-title`을 주고, 작업 폴더는 턴마다 새로 만드는 빈 폴더 `<home>/chat/cwd/<턴>`(턴이 끝나면 지움), 설정은 덧씌우는 파일 `<home>/chat/omp-chat.yml`입니다. 이 파일은 omp의 설정 발견 소스를 모두 끄고(`disabledProviders`: MCP 서버, 플러그인, hook, skill이 아무것도 안 붙음), 메모리, autolearn, advisor도 끕니다. 이 `disabledProviders` 목록은 chat omp에 한해 사용자의 omp `disabledProviders`를 대신합니다. `--no-tools`만으로는 사용자 MCP 서버의 도구가 오히려 최상위 도구로 붙는 것을 실제 omp 18.22.2로 확인했습니다. `--approval-mode yolo`는 주지 않고 moraebot의 host tool도 하나도 등록하지 않습니다. 프롬프트를 보내기 전에 `get_state`로 도구 목록을 읽어 하나라도 있으면 그 턴을 거절합니다(`E_CHAT_TOOLS`). 세션은 대화마다 `<home>/chat/<대화>/session`에 따로 두고 lead 세션과 섞지 않습니다. chat 메시지는 inbox에 저장되지 않고 lead에 끼어들지도 않습니다. chat omp는 동시에 2개까지만 돌고, 나머지 턴은 자리가 날 때까지 기다립니다. 메신저가 같은 chat 메시지를 다시 보내면(최근 1024개 메시지 id 기준) 한 번만 답합니다. 채널에 바로 올린 chat 메시지는 스레드를 먼저 엽니다. 채널에 바로 올라온 봇 메시지는 매번 새 스레드 키를 받으므로 봇 답장 상한을 채널 단위로 셉니다. 첨부 파일은 `<home>/chat/<대화>/files/`에 잠시 받아 프롬프트에만 넣고(5 MB 이하 그림은 이미지로, 64 KiB 이하 글 파일은 본문에, 나머지는 이름만. 한도를 넘는 파일은 내려받지도 읽지도 않음) 턴이 끝나면 지웁니다. 주인의 작업 폴더에는 들어가지 않습니다. 종료할 때는 lane처럼 chat omp도 모두 죽입니다.

> **경고: omp는 yolo 모드로 돕니다.** 주인 대신 lead와 말할 수 있는 사람은 사실상 이 컴퓨터에서 아무 명령이나 실행할 수 있습니다. 파일 삭제, 비밀번호 파일 읽기, 외부로 데이터 보내기도 됩니다. 그래서 chat 역할은 lead에 닿지 않게 만들고, 접근 설정이 유일한 안전장치라는 점은 그대로입니다. `allow`에는 정말 믿는 사람과 봇만 넣으세요.

검사는 두 번 합니다. 메시지를 받을 때 한 번, 큐에서 꺼내 omp로 보내기 직전에 한 번입니다(dinobot 방식). 그 사이에 주인이 권한을 뺐을 수 있기 때문입니다.

## 3. omp에 전달하기 (개발자용)

> omp-rpc 의존성: oh-my-pi v18.20.2 커밋(7c49c11a)을 git 의존성으로 직접 받습니다. 복사본(vendor)은 쓰지 않았습니다.

이 장은 구현할 사람을 위한 세부 내용입니다. 주인은 3.2절의 yolo 예외 설명과 3.5절의 첫 항목만 읽어도 됩니다.

### 3.1 용어

- **RPC 모드**: omp를 화면 없이 실행하고, 명령과 결과를 글자 줄로 주고받는 방식입니다. 한 줄에 JSON 객체 하나가 들어갑니다(JSONL).
- **stdin / stdout**: 자식 프로세스의 입력 통로와 출력 통로입니다. 게이트웨이는 stdin에 명령을 쓰고 stdout에서 결과를 읽습니다.
- **세션 폴더**: 대화 기록 파일이 저장되는 폴더입니다. lead는 `<홈>/lead/session`, lane은 `<홈>/lanes/<id>/session`을 씁니다.

### 3.2 실행 명령

lead omp는 하나만 띄웁니다(3.7절). lane도 같은 모양의 명령으로 띄우고, 세션 폴더만 `<홈>/lanes/<id>/session`으로 다릅니다.

```
omp --mode rpc --session-dir <홈>/lead/session --cwd <작업 폴더> --approval-mode yolo
```

`--cwd`는 omp가 일하는 폴더입니다. yolo에도 예외가 하나 있습니다. omp 설정의 "제한 폴더" 안에서 하는 작업은 yolo여도 승인을 요청하고, 화면이 없으면 거부됩니다.

### 3.3 흐름

```
 전체 대기열에서 메시지 하나 꺼냄
          |
          v
 [자식 없음] omp 실행 --> stdout에 ready
          |
          +--> negotiate_protocol (버전 2)
          +--> open_session {세션 폴더}
          +--> prompt {id = Discord 메시지 id}
          |
 stdout 이벤트: agent_start, message_update, tool_execution_* ...
          +--> Discord "입력 중" 표시
          |
 prompt_result {id, status}
          +--> 이벤트에서 모은 답장 글을 Discord에 보냄, SQLite에 done 기록
```

1. **시작.** omp는 켜지자마자 stdout에 `ready` 한 줄을 씁니다. 지원하는 통신 규칙(프로토콜) 버전(1과 2)이 들어 있습니다. 게이트웨이는 곧바로 버전 2를 요청합니다(`negotiate_protocol`). 버전 2에서는 1 MiB(약 100만 바이트)가 넘는 큰 결과도 조각으로 나뉘어 잃지 않고 도착합니다.
2. **대화 열기.** `open_session`으로 세션 폴더를 엽니다. 이때 넘기는 `sessionDir`는 실행할 때 `--session-dir`로 준 `<홈>/lead/session`과 같은 폴더입니다. 기록이 있으면 이어 가고, 없으면 새로 시작합니다. 응답의 `resumed` 값으로 어느 쪽인지 알 수 있습니다. `--continue` 옵션과 같은 일을 실행 중에 하는 것입니다.
3. **메시지 보내기.** `prompt` 명령의 `id`에 Discord 메시지 id를 넣습니다. 응답은 순서대로 오지 않을 수 있으니, 순서가 아니라 `id`로 맞춥니다.
4. **진행 중.** omp는 일하는 동안 이벤트를 계속 보냅니다. 게이트웨이는 이걸 보고 Discord에 "입력 중" 표시를 유지합니다. Discord "입력 중" API의 유지 시간은 **미검증**입니다(열린 질문 8).
5. **끝.** 같은 `id`의 `prompt_result`가 오면 omp가 그 메시지에 대한 한 턴을 마친 것입니다. `status`는 `completed`, `aborted`, `error` 중 하나입니다. 모델 쪽에서 실패해도 명령 응답은 성공으로 오고, 실패는 `status: error`로만 나타납니다. 그래서 보통은 명령 응답이 아니라 `prompt_result`를 기다립니다.
   - **예외: `agentInvoked`가 `false`일 때.** `prompt` 명령 응답의 `data.agentInvoked`가 `false`이면 `prompt_result`가 오지 않습니다(omp 자체 슬래시 명령 등). 이때는 그 명령 응답이 끝 신호입니다. 이 경우를 처리하지 않으면 게이트웨이가 영원히 기다립니다.
   - **`sessionSettled`가 `false`일 때.** `prompt_result`가 와도 백그라운드 작업이 남아 있으면 나중에 omp를 다시 깨울 수 있습니다. 그래서 "턴이 끝났다"와 "omp가 완전히 조용해졌다"는 다릅니다.
6. **답장 글 모으기.** 답장할 글은 `prompt_result`에 들어 있지 않습니다. `prompt_result`에는 상태 같은 정보만 있습니다. 답장 글은 omp가 일하는 동안 보내는 이벤트에서 모읍니다. 공식 Rust 클라이언트를 쓰면 `assistant_text`로 받을 수 있습니다.

### 3.4 일하는 중에 새 메시지가 오면

omp가 답을 만드는 중에 `prompt`를 보내려면 `streamingBehavior`를 꼭 넣어야 합니다. 빠뜨리면 명령이 실패합니다.

| 방식 | 뜻 | 언제 전달되나 |
|---|---|---|
| `steer` | 지금 하는 일에 끼어들어 방향을 바꿈 | 가능하면 즉시, 아니면 다음 도구 묶음이나 턴 경계 |
| `followUp` | 지금 일이 끝난 뒤 이어서 처리 | omp가 원래 멈출 시점 |
| `abort` | 지금 일을 멈춤 | 별도 명령. 결과는 `status: aborted`로 옴 |

`steer`와 `followUp`은 `prompt`의 `streamingBehavior` 값입니다. 같은 이름의 별도 명령(`steer`, `follow_up`)도 있습니다. 기본은 `followUp`입니다. Discord의 `/stop`은 `abort`로 보냅니다. 큐에 남은 메시지까지 돌려받고 멈추려면 `abort_and_restore_queue`를 씁니다.

### 3.5 자식 프로세스 관리 규칙

- **한 세션에 자식은 하나만.** 같은 세션 기록(세션 id)에 omp 두 개가 동시에 쓰면 오류가 나지 않습니다. 대신 두 번째 프로세스가 기록을 새 파일로 복사해 따로 씁니다. 대화 기록이 경고 없이 둘로 갈라집니다. 이걸 막는 것은 게이트웨이의 책임입니다.
- **다시 띄우기 전에 완전히 정리.** 새 자식을 띄우기 전에 옛 자식을 종료하고, 정말 끝났는지 확인합니다. 이것을 reap(끝난 프로세스를 운영체제에서 거둬 가는 일)이라고 합니다.
- **stdout은 끝까지 읽기.** 게이트웨이가 읽기를 멈추면 omp는 출력을 임시 파일에 쌓습니다. 종료가 무한정 늦어지고 디스크가 찰 수 있습니다. stdin을 닫은 뒤에도 계속 읽습니다.
- **정상 종료.** stdin을 닫으면 omp는 받은 명령을 마저 처리하고 코드 0으로 끝납니다.
- **종료 코드 1은 경고.** 세션 저장 실패가 정리 단계까지 남으면 omp는 코드 1로 끝납니다. 출력 실패도 치명적입니다. 마지막 턴이 기록되지 않았을 수 있으므로, 그 메시지를 "확인 필요"로 남기고 주인에게 알립니다.
- **남는 백그라운드 작업.** bash 도구가 백그라운드로 남긴 작업은 omp 프로세스 그룹 밖에 있어서, omp를 꺼도 같이 꺼지지 않습니다.

### 3.6 Rust 클라이언트

omp 저장소에는 공식 Rust 클라이언트 `sdk/rust/omp-rpc`가 있습니다. 메시지 형식 코드는 omp 스키마에서 자동으로 만들어지고, `ready` 기다리기, 버전 2 요청, 큰 결과 조각 합치기, `prompt_result` 기다리기를 대신 해 줍니다. 종료할 때는 프로세스 그룹 전체에 SIGTERM(종료 요청 신호)을 보내고, 1초 뒤에도 남아 있으면 SIGKILL(강제 종료 신호)을 보낸 다음 거둡니다.

주의할 점이 두 가지 있습니다.

- **crates.io의 `omp-rpc`가 아닙니다.** crates.io는 Rust 라이브러리를 내려받는 공개 저장소입니다. 저장소의 크레이트(Rust 라이브러리 단위)는 `publish = false`라서 crates.io에 없습니다. crates.io에 있는 같은 이름은 다른 것이거나 출처가 확인되지 않았습니다. 그래서 git 주소와 특정 커밋으로 고정해서 씁니다.
- **기다리는 방식입니다.** 이 클라이언트는 결과가 올 때까지 호출한 쪽을 멈춰 세웁니다. 게이트웨이는 tokio(여러 일을 동시에 처리하는 Rust 실행 환경)로 돌기 때문에, 이 클라이언트는 별도 작업 흐름(스레드)이나 `spawn_blocking`(오래 기다리는 일을 따로 돌리는 tokio 기능)에서 불러야 합니다.

### 3.7 lead와 lane (slice 4)

**lead 자식 하나.** omp RPC 자식 하나(lead)가 지금 켜진 메신저의 모든 스레드와 DM을 맡습니다. 세션 폴더는 `<홈>/lead/session`, stderr 기록은 `<홈>/logs/omp-lead.stderr.log`입니다. omp 세션 id는 `sessions` 표의 `name='lead'` 줄에 저장하고, 재시작하면 그 세션을 다시 엽니다. 다른 프로세스가 세션을 쥐고 있으면 `E_SESSION_BUSY`로 그 메시지를 "확인 필요"(`needs_check`)로 둡니다. 대화가 바뀌어도 lead를 끄지 않습니다. 메시지는 전체 대기열 하나에서 먼저 온 순서로 꺼냅니다.

**봉투(envelope) 줄.** lead에 넘기는 입력은 모두 어느 대화에서 왔는지 알리는 영어 한 줄로 시작하고, 줄바꿈 뒤에 본문이 옵니다.

```
[moraebot conversation=<key> message=<row id>]           주인 메시지 (본문 + 첨부 파일 목록)
[moraebot conversation=<key> lane=<id> result]           lane이 낸 결과 (본문 = lane의 마지막 글, 완료 주장 없음)
[moraebot conversation=<key> lane=<id> done]             완료 확인을 통과한 lane 결과 (본문 = lane의 마지막 글 + 확인 기록)
[moraebot conversation=<key> lane=<id> failed code=<CODE>]  lane 실패 (본문 없음, 단 E_DONE_UNMET은 lane의 마지막 글 + 확인 기록)
```

lead의 답은 그 입력의 줄(row)에 저장하고 `reply_target(conversation_key)`로 보냅니다. lane 결과에 대한 답은 lane을 시작한 대화로 갑니다. "마지막으로 말한 대화" 같은 상태는 어디에도 없습니다.

**역할 안내문.** 기본 안내문 `lead::DEFAULT_GUIDANCE`(영어)를 메신저 안내문과 함께 `--append-system-prompt=` 하나로 넘깁니다. 모양은 `<안내문>\n\n<메신저 안내문>`이고, 둘 다 비면 그 인자를 넣지 않습니다. `<홈>/hints/lead.md`가 있으면 기본 안내문을 바꿉니다. 규칙은 메신저 안내문과 같습니다(8192바이트까지, 빈 파일은 끄기, 너무 크면 `E_HINTS_FILE`).

**lane 도구 다섯 개.** lead는 게이트웨이가 `send_file` 옆에 등록한 호스트 도구로만 lane을 다룹니다.

| 도구 | 하는 일 |
|---|---|
| `lane_start {task, done}` | lane을 저장하고 따로 omp 자식을 띄운 뒤 `lane_id`를 바로 돌려줍니다. 같은 `toolCallId`로 두 번 불려도 lane은 하나입니다 |
| `lane_message {lane_id, text}` | 일하는 lane이면 지금 턴에 `prompt{streamingBehavior:"steer"}`로 끼워 넣고, 일하지 않는 lane이면 저장된 세션을 다시 열어 일을 더 줍니다 |
| `lane_status {lane_id?}` | 저장소에 있는 lane 목록과 상태를 돌려줍니다. 닫힌 lane은 `closed`와 `close_reason`으로 보입니다 |
| `lane_close {lane_id, reason}` | 일하지 않는 lane을 닫고, `repo` lane이면 폴더 정리를 해 봅니다(3.13) |
| `lane_stop {lane_id, reason}` | 일하는 lane의 지금 일을 바로 끊습니다. lane은 열린 채로 남습니다(3.16) |

거절은 도구 오류 `"<CODE>: <이유>"`로 돌아갑니다. 동시에 일하는 lane이 `MAX_RUNNING_LANES`(3)개면 `E_LANE_LIMIT`, 일하는 중인 lane에 `lane_message`는 그 턴에 steer로 들어가고(거절 아님), 일하는 중인 lane을 닫으려 하면 `E_LANE_BUSY`, 닫힌 lane에 `lane_message`는 `E_LANE_CLOSED`, 모르는 id는 `E_LANE_UNKNOWN`, 일하지 않는 lane을 끊으려 하면 `E_LANE_IDLE`, 잘못된 인자는 `E_LANE_ARGS`, 게이트웨이가 꺼지는 중이면 `E_LANE_STOPPING`(lane 자식을 띄우지 않음)입니다. 도구 처리기는 lane 일을 기다리지 않습니다. lane 자식은 `repo` 없이 시작했으면 lead와 같은 작업 폴더에서, 있으면 자기 git worktree에서 돌고(3.11), 세션 폴더는 `<홈>/lanes/<id>/session`이고, done 줄이 있는 lane은 호스트 도구 `lane_done` 하나만 받으며(3.8), done 줄이 없는 lane은 호스트 도구가 없습니다. lane은 메신저와 직접 말하지 않습니다. 답을 낸 lane은 자식을 끄고 기록만 남깁니다.

**결과 줄(result row).** lane이 끝나면 `finish_lane`이 한 트랜잭션에서 inbox에 줄 하나(`platform = 'lane'`, `message_id = "<lane id>:<순번>"`, `origin = 'lane'`, `queued`)를 넣고 lane 상태를 바꿉니다. `UNIQUE(platform, message_id)` 덕분에 결과는 한 번만 들어갑니다. 결과 줄은 주인 확인 없이 전체 대기열을 따라 lead에게 갑니다.

**steer 묶음 규칙.** lead가 어떤 대화의 턴을 처리하는 중에 **같은 대화**에서 새 메시지가 오면, 그 메시지를 `prompt{streamingBehavior:"steer"}`로 지금 턴에 끼워 넣습니다. 조건은 모두 지켜야 합니다.

- 파일 저장이 끝났고, lead가 그 턴의 `agent_start`를 보낸 뒤입니다.
- 그 줄이 턴의 주 메시지보다 새것이고, 같은 대화에 더 오래된 `queued` 줄이 없습니다(`steer_into`의 조건부 UPDATE). 그래서 끼워 넣은 메시지가 먼저 보낸 메시지를 앞지르지 않습니다.
- 한 턴에 하나까지입니다. 그 뒤에 온 같은 대화 메시지는 다음 턴에 갑니다.

**lane steer 규칙.** lead가 일하는 lane에 `lane_message`를 보내면 그 글은 새 프롬프트가 아니라 지금 도는 턴에 `prompt{streamingBehavior:"steer"}`로 들어갑니다. lead 턴의 steer와 같은 omp 경로이고, 조건은 이것입니다.

- lane 자식이 살아 있고, 그 job의 `stop`이 아직 없습니다(끊는 중이면 steer하지 않습니다).
- job의 steer 우편함은 워커가 결과를 쓰고 `inner` 아래에서 job 항목을 지울 때 함께 사라집니다. `lane_message`도 `inner` 아래에서만 보내므로, 우편함에 들어간 글은 턴에 steer로 들어가거나 아래처럼 다음 프롬프트로 돕니다. 잃어버리지도, 두 번 돌지도 않습니다.
- steer는 그 턴의 첫 `agent_start` 뒤에만 나갑니다. omp는 이미 스트리밍 중인 run에만 steer를 받으므로, 그 전에 도착한 글은 게이트가 열릴 때까지 기다립니다(lead의 steer 게이트와 같은 신호).
- 순서는 `lane_message`가 도착한 순서입니다. omp는 steer를 주 run 안에서 답하고, run이 끝나는 순간에 닿은 steer는 따로 run을 열어 답합니다. job은 보낸 steer마다 그 결과를 기다리고(주 프롬프트 뒤 최대 턴 제한까지, 넘으면 lane 자식을 죽임) 더 새 답을 결과로 씁니다. 그래서 자식이 job보다 오래 살지 않고, 결과 줄은 여전히 하나(`<lane id>:<순번>`)이며 `prompts`도 늘지 않습니다.
- 턴이 받지 못한 글(게이트가 열리기 전에 턴이 끝났거나, 턴이 끝난 뒤 결과 저장 전에 도착한 글)은 job 항목을 지운 직후 도착 순서대로 이어 붙여 lane의 다음 프롬프트로 돌립니다. 일하지 않는 lane에 `lane_message`를 보낸 것과 같습니다. lead가 끊은 job이나 종료로 끊긴 job이면 돌리지 않고 개수와 길이만 로그에 남깁니다.
- steer 글에는 lane의 꼬리말(`lane_done` 안내, `lane_report` 안내, git 안내)을 붙이지 않습니다. 그건 주 프롬프트에만 들어갑니다.

**moraebot 노트(rowless steer).** moraebot이 스스로 lead의 진행 중인 턴에 글을 넣을 때는 `LeadNotes`(`OmpManager::notes()`)를 씁니다. 지금은 recall advisor의 mid-task 선택(`memory.recall.mid_task = "on"`, 기본값), 바쁜 lead 메모(3.18절), 같은 대화 lane의 진행 소식(3.15절)이 씁니다.

- 노트는 `prompt{streamingBehavior:"steer"}` 하나이고 첫 줄이 `NoteKind`의 태그 줄입니다(`[moraebot memory recalled]`, `[moraebot lead busy]`, `[moraebot lane news]`). `conversation=`이 없으므로 `lead::parse_tag`는 주인 입력이나 lane 결과로 읽지 않습니다.
- inbox 줄이 없고 따로 답을 받지 않습니다. 주인 steer 자리(`OpenTurn::steer`)를 쓰지 않으므로 같은 대화의 다음 주인 메시지는 그대로 끼워 넣어집니다.
- 노트는 `TurnId`로 그 턴에만 들어가고, 그 턴의 첫 `agent_start` 뒤에만 나갑니다(`opens_steer_gate`, lane steer와 같은 신호). 턴이 이미 끝났거나 아직 시작하지 않았으면 보내지 않고, 부른 쪽이 버립니다.
- `prompt`는 주 프롬프트와 주인 steer 다음에 노트의 결과를 같은 턴 제한 안에서 기다리고 답 글은 버립니다. 노트는 열린 턴에만 있으므로 턴 종료, 셧다운, lead 재시작을 넘어 남지 않습니다.

다른 대화의 메시지와 lane 결과는 끼워 넣지 않고 다음 턴을 기다립니다. 워커와 steer 경로는 모두 `claim`/`steer_into`로 줄을 차지하므로 한 줄은 한 경로에서만 돕니다. 묶음의 답은 하나로 합쳐 주 메시지 줄에 저장하고(`finish_group`, 끼워 넣은 줄은 `done`), 한 번 보냅니다. 묶음 중 하나라도 실패하거나 끊기면 묶음 전체가 `failed`(`fail_group`) 또는 `needs_check`(`interrupt_group`)가 됩니다.

**재시작 규칙.** 무엇도 두 번 돌리지 않습니다.

- 시작할 때 `processing`으로 남은 줄(끼워 넣은 줄 포함)은 `needs_check`가 되고, 같은 대화의 대기 중인 주인 줄(`origin = 'owner'`)을 붙잡아 둡니다. 대기 중인 lane 결과 줄은 붙잡지 않습니다.
- 시작할 때 `running`인 lane은 `needs_check` / `E_LANE_INTERRUPTED`가 되고, lane을 시작한 대화에 `N_LANE_INTERRUPTED` 안내를 한 번 보냅니다(`lanes.notice_sent`). 시간 초과처럼 스스로 멈춘 lane은 이미 `failed code=` 결과 줄로 알렸으므로 이 안내를 받지 않습니다.
- lane을 다시 돌리지 않고, lead 입력을 다시 보내지도 않습니다.
- 끄는 순서는 디스패처 종료, `omp.shutdown_all()`(lead와 lane 모두 끄기), `recover_on_start`입니다. 꺼지는 중에 스스로 끝난 lane의 결과는 저장해 두었다가 다음에 켜질 때 lead에게 보냅니다.

**업그레이드.** slice 3의 `inbox.sqlite`는 그 자리에서 바뀝니다(새 표 `sessions`, `lanes`, 새 열 `origin`, `steered_into`). `<홈>/conversations/*/session` 폴더는 읽지도, 옮기지도, 지우지도 않습니다. lead는 빈 기록으로 시작합니다.

### 3.8 완료 확인 (slice 5)

**done 줄.** `lane_start`는 `done` 문자열을 꼭 받습니다(Dori mode 문법). 확인 방법은 여섯 가지입니다. `command ["program","arg"]`는 그 프로그램이 종료 코드 0으로 끝나면 통과하고, 뒤에 `contains "글자"`를 붙이면 일반 출력(stdout)에 그 글자도 그대로 있어야 통과합니다. `file <경로>`는 그 경로가 있으면 통과하고, 뒤에 `contains "글자"`를 붙이면 파일 안에 그 글자도 그대로 있어야 통과합니다. `url <주소>`는 그 주소가 200번대로 답하면 통과합니다. `merged <주인>/<저장소>#<번호>`(예: `merged acme/app#12`)는 GitHub에 그 PR이 합쳐졌는지 물어 합쳐졌을 때만 통과합니다. 글자는 늘 JSON 문자열로 적습니다. 주소는 `http`, `https`만 받고, 아이디나 비밀번호가 든 주소, 뒤에 무엇이 더 붙은 주소는 받지 않습니다. 잘못된 `contains` 부분은 `BadText`, 잘못된 주소는 `BadUrl`, 잘못된 PR 표기는 `BadPr`로 밝힙니다. `closed`, `published`는 아직 받지 않습니다. 여러 개는 `;`로 잇습니다. `done`이 없거나 비면 `E_LANE_ARGS`, 확인할 수 없는 줄이면 잘못된 부분을 밝혀 `E_DONE_SYNTAX`로 거절합니다. 거절된 호출은 lane을 저장하지도, 자식을 띄우지도 않습니다. 받아들인 줄은 정리된 모양(canonical)으로 `lanes.done` 열에 저장하고 `lane_status`에도 보여 줍니다. 이 열이 없던 slice 4 저장소는 열 때 열이 더해지고, 예전 lane은 done 줄이 없는 lane으로 남습니다(도구도, 안내 꼬리말도 없고 결과는 `result` 표시).

**`lane_done` 도구.** done 줄이 있는 lane은 호스트 도구 `lane_done {evidence}` 하나를 받고, 보내는 프롬프트마다 끝에 done 줄과 완료를 주장하는 방법을 적은 꼬리말이 붙습니다. lane이 일이 끝났다고 보면 증거(한 일과 확인한 방법)를 담아 `lane_done`을 부릅니다. 그러면 moraebot이 그 도구 호출 안에서 모든 확인을 차례로 돌립니다. 확인은 이 호출 안에서만 돕니다.

- `command`는 셸 없이 argv 그대로, 작업 폴더에서, 자기 프로세스 그룹으로 돕니다. 환경은 omp 자식과 같고 메신저 변수만 뺍니다(`omp::check_env`). 입력은 닫고, 출력은 파이프가 아니라 `<홈>/lanes/<id>/check/<순번>/stdout`, `stderr` 파일(권한 0600)로 보냅니다. `CHECK_TIMEOUT`(120초)이 지나면 그룹째 죽입니다. 끝나면 그룹에 남은 프로세스도 죽입니다. `setsid`로 새 세션에 빠져나간 프로세스는 닿지 않습니다.
- `command ... contains`는 명령이 0으로 끝난 뒤에만 stdout 파일 전체에서 글자를 찾습니다. 0이 아니면 찾지 않고 위와 같은 코드로 실패합니다. stderr는 보지 않습니다.
- `file`은 경로가 있는지 봅니다. 상대 경로는 작업 폴더 기준이고, `~/`는 `HOME`으로 풉니다. `contains`가 있으면 파일 전체를 64KiB(`SCAN_CHUNK`)씩 읽으며 바이트로 비교하고, 조각 경계에 걸친 글자도 찾습니다. 크기 제한은 없습니다.
- `url`은 GET 한 번을 보냅니다. 다른 주소로 넘기는 답(301, 302 등)을 따라가지 않고, 프록시 설정을 쓰지 않으며, 제한 시간은 명령과 같은 `CHECK_TIMEOUT`(120초)입니다. 답의 본문은 읽지 않고 상태 번호만 봅니다. 프로세스를 띄우지 않습니다.
- `merged`는 `https://api.github.com/repos/<주인>/<저장소>/pulls/<번호>`에 GET 한 번을 보내고 답의 `merged` 값을 봅니다. `url`처럼 다른 주소로 넘기는 답을 따라가지 않고(저장소 이름이 바뀌면 `E_CHECK_STATUS`), 프록시 설정을 쓰지 않으며, 제한 시간은 `CHECK_TIMEOUT`(120초)입니다. `gh` 같은 프로그램은 띄우지 않습니다.

확인 하나의 결과 코드는 통과(코드 없음), `E_CHECK_EXIT`, `E_CHECK_MISSING`, `E_CHECK_TIMEOUT`, `E_CHECK_SPAWN`, `E_CHECK_TEXT`(글자가 없음), `E_CHECK_READ`(있는 파일이나 stdout 파일을 읽을 수 없음), `E_CHECK_STATUS`(주소가 200번대 밖으로 답함, 내용은 `status <번호>`), `E_CHECK_HTTP`(접속, TLS, 주소 찾기 등 요청 자체가 실패, 또는 GitHub 답이 예상한 JSON이 아님), `E_CHECK_UNMERGED`(PR이 아직 합쳐지지 않음), `E_CHECK_NOT_FOUND`(GitHub가 404로 답함, PR이 없거나 토큰으로 볼 수 없는 비공개 저장소), `E_CHECK_AUTH`(GitHub가 토큰을 거절함(401, 403), 또는 토큰을 보낼 수 없음), `E_CHECK_RATE`(GitHub 요청 한도를 다 씀) 중 하나입니다. 주소가 제한 시간 안에 답하지 않으면 `E_CHECK_TIMEOUT`입니다. 로그에는 lane id, 확인 개수, 실패 코드만 남기고 증거나 확인 출력은 남기지 않습니다.

**GitHub 토큰.** `merged`는 확인 환경(`omp::check_env`)에서 `GH_TOKEN`을, 없거나 비면 `GITHUB_TOKEN`을 씁니다. 같은 이름이 여러 번 있으면 마지막 것을 씁니다. 토큰은 요청 머리의 `Authorization`에만 실리고, 결과 내용, 로그, 저장소에는 남지 않습니다. 보이는 ASCII가 아닌 글자가 든 토큰은 요청 없이 `E_CHECK_AUTH`입니다. 토큰이 없으면 토큰 없이 묻습니다. 이때는 공개 저장소만 보이고 GitHub가 한 시간에 60번까지만 받습니다. launchd, systemd 서비스는 PATH와 홈 위치만 넘기므로 토큰이 전달되지 않습니다. 그래서 서비스로 돌리면 공개 저장소만 확인되고, 비공개 저장소는 토큰이 있는 터미널에서 `moraebot run`으로 돌릴 때만 확인됩니다.

**dori와 다른 점.** 글자는 정규식이 아니라 그대로 포함되는지만 봅니다. 명령은 stdout만 보고, 주소는 본문을 보지 않고 상태 번호만 봅니다. dori와 달리 주소 확인에는 120초 제한이 있고, 다른 주소로 넘기는 답을 따라가지 않습니다.

**주장 상태(claim).** 작업(job)마다 `None`, `Failed`, `Accepted` 중 하나를 가지며 `None`에서 시작합니다. 모두 통과하면 `Accepted`가 되고 도구는 받아들였다고 답합니다. 하나라도 실패하면 `Failed`가 되고, 도구는 `E_DONE_UNMET`과 실패한 확인마다 한 줄(확인, 코드, 자세한 내용)로 거절합니다. lane은 같은 턴에서 계속 일하고 `lane_done`을 몇 번이든 다시 부를 수 있습니다. 턴의 마지막 호출이 결과를 정합니다. moraebot은 lane에 스스로 프롬프트를 보내지 않습니다. 한 lane의 `lane_done` 호출은 하나씩 차례로 돌고, 잠금 순서는 claim, `inner`, 저장소입니다.

**결과 표시.** 턴이 답으로 끝나면 주장 상태에 따라 결과 줄을 씁니다.

| 마지막 주장 | lead가 받는 표시 | lane 상태 |
|---|---|---|
| 통과 | `done` (답 + 증거와 모든 확인을 담은 기록) | `idle` |
| 실패 | `failed code=E_DONE_UNMET` (답 + 기록) | `failed` / `E_DONE_UNMET`, 다시 `lane_message` 가능 |
| 없음 | `result` (답, 확인하지 않음) | `idle` |

`Failed`나 `NeedsCheck`로 끝난 턴은 주장과 상관없이 slice 4처럼 알립니다. `lane_message`로 다시 일을 주면 저장된 done 줄로 같은 도구와 꼬리말을 붙입니다.

**끄기와 재시작.** 꺼지기 시작한 뒤의 `lane_done`은 확인을 시작하지 않고 `E_LANE_STOPPING`으로 거절합니다. `shutdown_all`은 lane 자식을 모두 먼저 죽이고, 그다음 돌고 있는 확인을 죽입니다. 그래서 확인의 거절이 살아 있는 lane에 닿는 일이 없고, 확인 도중 끊긴 턴은 늘 잘린 턴이 됩니다(결과 줄 없음, 다음 시작 때 `E_LANE_INTERRUPTED` 안내 한 번). `shutdown_all`은 죽인 확인이 모두 거둬진 뒤에(`ABORT_JOIN_BOUND`까지) 돌아옵니다. 시간 초과나 자식을 잃어 작업이 먼저 끝나면, 그 작업의 워커가 자기 확인을 죽입니다. 보통 종료에서는 확인 프로세스도, 확인의 프로세스 그룹에 남은 프로세스도 확인이나 게이트웨이보다 오래 살지 않습니다. SIGKILL로 죽은 게이트웨이는 정리하지 못합니다.

### 3.9 첫 만남과 persona (light)

**시작.** `moraebot init`이 새 설정을 쓰면 `<홈>/onboarding.request` 파일(권한 0600)을 남깁니다. 이미 쓰던 홈에서는 `moraebot onboarding`이 같은 파일을 만들고 `{"onboarding":"requested"}`를 출력합니다. 이 명령은 저장소를 열지 않습니다.

`moraebot onboarding --greet dm|channel`은 요청 파일에 `dm` 또는 `channel` 낱말 하나를 쓰고, 출력에 `"greet"` 값을 함께 넣습니다. 기본은 `dm`이고, `moraebot init`도 `dm`을 씁니다.

**인사 한 번.** 디스패처가 켜질 때(`startup`) 요청 파일이 있으면, 먼저 파일의 낱말을 읽습니다(`dm`/`channel`, 비었거나 모르는 낱말은 `dm`). 그다음 `onboarding` 표의 줄을 `active`로 만들고 파일을 지운 뒤, 정해진 인사(`N_ONBOARDING_HELLO`)를 보냅니다. `dm`이면 `open_dm`으로 주인 DM을 열어(Slack은 사용자 id 그대로, Discord는 `create_dm_channel`) 그곳에 보내고, 열기나 보내기가 실패하면 설정된 채널에 한 번 보냅니다. `channel`이면 채널에만 보냅니다. 파일을 지우지 못하면 `E_ONBOARDING_REQUEST`를 로그에 남기고 인사하지 않습니다. 두 곳 다 실패해도 다시 보내지 않습니다. 파일이 없으면 아무 일도 없습니다.

**주인 DM.** 어댑터의 `open_dm(user_id)`는 그 사용자와 봇의 DM으로 보낼 대상을 돌려줍니다. 열 수 없거나 DM이 없는 메신저면 `E_ADAPTER_DM`입니다. 주인이 DM에서 답하면 그 대화(`<메신저>:dm:<채널id>`)에서 첫 만남이 이어지고, 표시 줄 번호는 대화와 상관없이 시작 뒤 주인 메시지 수로 셉니다.

**표시 줄.** 첫 만남이 진행 중(`active`)이면 주인 메시지의 lead 입력 두 번째 줄에 `[moraebot onboarding turn=<n>]`이 붙습니다. `n`은 시작 뒤 주인 메시지 수(이번 것 포함)이고, 1부터 `MAX_TURNS`(10)까지만 붙습니다. 끼워 넣은 메시지(steer)와 lane 결과에는 붙지 않습니다. lead 기본 안내문이 이 줄을 보면 무엇을 물을지 알려 줍니다.

**저장 도구.** lead는 호스트 도구 `onboarding_finish {outcome, name, tone, owner}`로 첫 만남을 끝냅니다.

| 경우 | 결과 |
|---|---|
| 주인 메시지에 답하는 중이 아님 | `E_ONBOARDING_ORIGIN` |
| 진행 중인 첫 만남이 없음 | `E_ONBOARDING_STATE` |
| 인자가 잘못됨 | `E_ONBOARDING_ARGS` |
| 이름이 비었거나 줄바꿈이 있음 | `E_PERSONA_FIELD` |
| 파일이 2048바이트를 넘음 | `E_PERSONA_SIZE` |
| 키, 토큰, 비밀번호처럼 보이는 글 | `E_PERSONA_SECRET` |
| 파일을 쓰지 못함 | `E_PERSONA_WRITE` |
| `done` | `<홈>/persona.md`(권한 0600)를 쓰고 표를 `done`으로 |
| `skipped` | 파일은 건드리지 않고 표를 `skipped`로 |

`persona.md`는 `Name:`, `Tone:`, `Owner:` 세 줄입니다. `Tone:`과 `Owner:`는 값이 있을 때만 씁니다.

**lead에 붙이기.** lead가 켜질 때마다 `persona.md`를 새로 읽어 안내문 뒤에 빈 줄, `persona::HEADER`, 파일 내용 순서로 붙입니다. 파일이 없으면 아무것도 붙이지 않습니다. 2048바이트를 넘거나, 비밀처럼 보이거나, 보통 파일이 아니거나, UTF-8이 아니면 붙이지 않고 코드만 로그에 남깁니다. 주인이 손으로 고친 파일에도 같은 규칙을 씁니다. lead 턴을 시작할 때마다(`prompt`가 `live` 잠금을 잡은 직후, 요청을 보내기 전) `persona.md`와 핵심 기억을 다시 읽어 붙일 글을 만듭니다. 지금 lead가 켜질 때 받은 글(`Live.append`)과 달라도 바로 다시 켜지 않습니다. lead는 처음 받은 시스템 프롬프트를 그대로 지니고(`src/pin.rs`), 바뀐 줄만 그 턴 입력 끝에 `<<<moraebot-memory update>>>` 블록 하나로 넣습니다(`pin::next_notice`). 블록은 바뀔 때마다 한 번 들어가고, 글이 처음 것으로 돌아오면 앞선 블록을 거두는 되돌림 블록이 한 번 들어갑니다. 앞선 실행의 세션을 이어 받은 lead에도 무엇을 지녔는지 모르므로 되돌림 블록(글이 다르면 지금의 블록)을 한 번 넣습니다. 그래야 제공자의 프롬프트 캐시가 그대로 맞습니다. 시스템 프롬프트를 다시 정하는 것은 캐시가 어차피 식었을 때뿐입니다(`OmpManager::restart_reason`). 마지막 턴이 끝난 뒤 `[lead] cache_minutes`(빠지면 60분) 넘게 쉬었거나(`memory_idle`), 켜진 뒤 omp가 맥락을 압축했거나(`memory_compacted`), 주인이 `refresh memory`/`기억 새로고침`을 보냈을 때(`memory_requested`)입니다. 그때 쉬고 있는 lead를 닫고(`Live::close`) 같은 세션 폴더와 저장된 세션 id로 다시 켜며, 로그 `I_LEAD_RESTART`에 이유를 남깁니다. `cache_minutes = 0`이면 예전처럼 글이 바뀔 때마다 다시 켭니다(`unpinned`). 같은 글이면 다시 켜지 않습니다. 턴과 끼워 넣기(steer) 도중에는 잠금 때문에 확인하지 않습니다. 직전 턴 결과가 `sessionSettled: false`(백그라운드 작업이 세션을 다시 깨울 수 있음)였으면 기다리지 않고 지금 lead로 그 턴을 처리하며, settled로 끝난 턴 다음 턴에 다시 켭니다(`Live.settled`). 다시 켜는 방식을 쓰는 까닭은 세 가지입니다. omp RPC에는 대화 중 시스템 프롬프트를 바꾸는 명령이 없고(`get_state`로 읽기만 됨), 다시 켜도 대화 기록과 압축 상태가 이어지며(약 0.2초), 도중에 끄면 진행 중인 도구 호출과 끼워 넣은 메시지를 잃습니다(실험 기록 refresh.md). 거절된 파일은 persona가 없는 글로 보아 같은 규칙을 따르고, 경고는 거절 상태가 바뀔 때 한 번만 남깁니다. 다시 켜기에 실패하면 그 턴은 보통의 시작 실패와 같고 다음 턴에 다시 켭니다. 시스템 프롬프트를 다시 정하면 제공자 캐시가 한 번 무효가 됩니다. 턴마다 토큰 수를 `I_LEAD_USAGE`로 남기고, 최근 20턴의 캐시 적중률은 상태 답에 나옵니다(3.18절). `onboarding_finish` 결과는 `"applies":"next_turn"`입니다. persona는 lane에는 붙이지 않습니다. lane의 기억 블록도 같은 방식으로 첫 프롬프트 때의 것을 lane이 닫힐 때까지 지니고, 바뀌면 lane의 다음 프롬프트에 같은 블록을 넣습니다(`LaneManager::pin_memory`).

**저장소.** `inbox.sqlite`의 `onboarding` 표에는 줄이 많아야 하나입니다(`state`는 `active`/`done`/`skipped`, `after_row`는 시작 때의 마지막 줄 번호). 예전 데이터베이스도 그대로 열립니다.

### 3.10 기억

moraebot은 자기 기억 저장소(`~/.moraebot/memory`, git)를 둡니다. lead와 lane에서는 omp 자체 기억을 끕니다(`--config <home>/omp-overlay.yml`: `memory.backend: "off"`, `autolearn.enabled: false`). 오너 터미널의 omp 기억과 섞이지 않게 하기 위해서입니다(결정 L-D10 변경, 2026-11-20). 자세한 내용은 docs/memory.md에 있습니다.

**오버레이는 키 단위로 합쳐집니다.** omp는 `--config` 파일을 오너의 전역·프로젝트 설정 위에 키 단위로 합칩니다(우선순위: 실행 중 덮어쓰기 > `--config` > 프로젝트 > 전역). 그래서 오버레이는 두 키만 바꾸고, 오너의 모델과 provider 설정은 그대로 씁니다. omp 18.22.2의 `packages/coding-agent/src/config/settings.ts`(`#mergeOwnLayers`, `getProvenance`)에서 확인했습니다. RPC 모드가 기억을 기본 `off`로 두더라도 오너가 전역 설정에서 켜 두면(예: `memory.backend: mnemopi`) 켜지므로 오버레이가 필요합니다.

**시작할 때 확인.** omp가 실제로 적용한 설정은 moraebot이 읽을 수 없습니다. `omp config`는 `--config` 오버레이를 읽지 않고, RPC `get_state`에는 설정이 없습니다. 그래서 `moraebot run`은 시작할 때 모델을 부르지 않고 두 가지를 확인합니다(#39 대안).
- `omp config get <key> --json`으로 오버레이의 두 키를 omp가 아는지 봅니다. 모르면(omp가 키 이름을 바꾼 경우) 오버레이가 조용히 무시되어 omp 기억이 되살아나므로 `E_OMP_OVERLAY`로 시작을 거부합니다. omp가 `Unknown setting: <key>`로 답할 때만 "모른다"로 봅니다. 다른 실패(예: 읽지 못하는 설정 파일)는 omp의 오류 줄을 붙여 `E_OMP_OVERLAY`로 알립니다.
- 이 확인은 홈 폴더(`<home>`, 서비스의 WorkingDirectory)에서 omp를 실행합니다. omp는 작업 폴더의 프로젝트 설정(`.omp/config.yml` 등)도 읽으므로, 셸의 현재 폴더에서 돌리면 `check-config`와 서비스의 답이 달라질 수 있습니다(#54).
- `omp --version`이 합치기를 확인한 버전(`omp_overlay::CHECKED_OMP_VERSION`, 지금 18.22.2)과 다르면 `W_OMP_VERSION` 경고를 로그에 남깁니다. 시작은 합니다.
- `moraebot check-config`도 같은 두 가지를 확인해 `OK`/`WARN`/`FAIL omp overlay` 줄로 보여 줍니다. omp를 올린 뒤 서비스를 다시 띄우기 전에 확인할 수 있습니다.

오버레이 파일은 gateway가 열 때, 그리고 lead나 lane을 띄울 때마다 새로 씁니다(손으로 고치거나 lane 셸이 지워도 다음 spawn 전에 되돌아옵니다). 이름이 0.2.0의 `omp/config-overlay.yml`과 다르므로 아래 옛 배치 검사와 겹치지 않습니다.

**0.2.0에서 올라올 때.** 새 기억 저장소(#39, docs/memory.md)가 `~/.moraebot/memory/`를 씁니다. 옛 배치(`memory/system` 등, `omp/config-overlay.yml`)가 남아 있으면 moraebot은 시작을 거부하고 지울 경로와 `rm -rf` 명령을 한 줄로 알려 줍니다. 코드가 지우지는 않습니다. 데이터베이스의 옛 기억 표는 쓰지 않습니다.

### 3.10a Memory P2: correction capture, outcome memory, recall advisor, git trust, private prompt files

This subsection is in English (#39 P2); docs/memory.md has the owner-facing details, the settings and the log codes.

**Correction capture** (`src/lessons.rs`). When an owner row becomes a lead input (`Dispatcher::process_next`, or on arrival when it is steered into a running turn; the same row is saved and counted once) and `[memory.self] enabled`, `lessons::capture` checks the owner's own sentences (`memory::own_sentences`: not `>`-quoted lines or code fences) against `correction_patterns` (whole words in order, the last as a word start, `…` for a gap). A match is queued as an owner entry in `self/observations.md` (`Bucket::SelfObs`, source `row=<id>`) through `MemoryHandle::queue_add`, so the turn never waits for the write; the text is the whole message folded when it fits 280 characters, else the matching sentences cut to 280. Earlier owner entries there from the last 90 days, read from committed lines only (`MemoryHandle::trusted_entries`), are compared by Jaccard similarity of n-grams inside each word (bigrams for a word with a character past Latin, such as Hangul; trigrams for other words), at least 0.27, after the matched phrase's words and English function words are removed from both; when the new one makes 3, 6, 9, ... similar corrections, a `<<<moraebot-memory corrections: ...>>>` block (guard line, the newest five earlier entries and this one with ids, escaped) is appended to the lead input, asking the lead to offer the owner once to save a standing rule with `기억해: ...`. No model runs. `self/` is in no prompt block and no search (lanes never read it); it is in the recall pool, so the lead's advisor can offer an earlier correction. A `self/self.md` written by an older version is skipped by every file walk and left on disk.

**Outcome memory** (`src/outcome.rs`). When a lane result ends with a code, `LaneManager::remember_outcome` saves it if it failed: an error (`Outcome::Failed` or `NeedsCheck`), an unmet done claim (`E_DONE_UNMET`) or a stop (`E_LANE_STOPPED`). It queues one `prov = lane` entry (`Source::lane(id, seq)`) in the first of the lane's projects (`memory::lane_projects` of its `repo`), else in `projects/_general.md`. `outcome::text` builds the text from the code, the task's first line (120 characters) and the failure: the first line of an error's detail, the signals of the failed checks in an unmet claim's receipt, or the stop reason (200 characters); never the lane's words. It expires like every non-owner entry (30 days). A successful result saves nothing. The lane manager gets the memory handle with the lead's (`OmpManager::with_memory`).

**Recall advisor** (`src/recall.rs`, owner-facing details in docs/memory.md#the-recall-advisor). On by default: `RecallMode::On`, `mid_task` follows the mode, model `@smol`. The owner wake runs before the turn (at most `wait_ms`); a mid wake follows each lead tool call that starts or ends, while the lead's assistant text only updates the plan line. A wake calls the advisor only when `Offered::fresh` finds a candidate not yet offered in this lead generation, or one offered while unrelated and related now. `in_scope`: the conversation's own thread, projects, `_general`, `self/observations.md` and replaced entries; other conversations' threads only when the conversation key holds `:dm:` (the owner's DM); never core. At most `MAX_CALLS_AT_ONCE` (2) calls run; a wake over it is `W_RECALL_CAP reason=busy`. `Cooldown` admits a mid wake while fewer than `mid_max_delivered` notes went to that conversation within `mid_cooldown_minutes`, and is charged only when the lead took a note. `LatePicks` keep the picks of a call that missed the wait for the conversation's next owner input, only in the same lead generation and under `LATE_TTL` (30 minutes). `recall_ledger` rows carry `phase` (`owner` or `mid`; older rows read as `owner`). The removed keys `mid_max_wakes_per_turn` and `mid_min_interval_seconds` still load and are named in `W_CONFIG_IGNORED`. `recent recalls` / `최근 떠올린 기억` lists the last 10 ledger rows (`Store::recent_recalls`), and the status answer gets a recall line (modes, calls in the last hour, last delivered pick).

**Memory git trust** (`src/memory.rs`). `MemoryStore::open` and every `apply` first run `sanitize_git` (`.git` must be a plain folder; `.git/config` is rewritten to `canonical_git_config` when it differs, `W_MEMORY_GIT_CONFIG`; `.git/info/attributes`, `.git/commondir` and `.git/config.worktree` are removed) and `reconcile_head`. moraebot's last own commit is kept in memory while it runs; `<home>/memory.head` only carries it to the next run. A HEAD past it is saved as `.rejected/foreign-<secs>-<nanos>.patch` and reset (`W_MEMORY_FOREIGN_COMMIT`); a `memory.head` changed or removed while running is rewritten (`W_MEMORY_HEAD_TAMPERED`). At open, a missing or malformed `memory.head` trusts HEAD once and records it, and one naming a commit the repository no longer has does the same with `W_MEMORY_HEAD_PRUNED` (a purge or a restored backup; nothing is reset). Each batch is journalled to `<home>/memory.pending` (base commit, files, message) before its commit; at open `replay_journal` commits it when HEAD is still its base and drops it otherwise (`W_MEMORY_PENDING_DROPPED`). A failed commit runs `undo_batch` and turns every success of the batch into the commit's error. `run_git` clears the environment (keeping `PATH` and `HOME`), sets `GIT_CONFIG_NOSYSTEM`, `GIT_CONFIG_GLOBAL=/dev/null`, `GIT_DIR` and `GIT_COMMON_DIR` to `.git`, `GIT_NO_REPLACE_OBJECTS`, and passes `core.hooksPath=/dev/null`, `core.fsmonitor=false`, `core.useReplaceRefs=false`, `/dev/null` excludes and attributes files, a fixed identity and no signing. Limits while lanes share the Unix user: a process that runs git in the folder itself can still commit an owner line, and while moraebot is stopped anyone who can write the home can replace or delete `memory.head` or plant a `memory.pending`.

**Saves stay internal (#56).** No memory save is posted in a conversation: replies carry only the lead's text, and a failed or interrupted turn stores nothing to post later. The memory writer logs each new entry as `I_MEMORY_SAVED` (id, file and bucket, never the text); the owner sees what was saved through the memory listing (`뭐 기억해?`). `Store::open` drops the `memory_notices` table an older version used for these lines, and the `memory_jobs` and `memory_cursor` tables of the removed observer.

**Plain-language memory requests.** `Dispatcher::memory_command` matches the owner's text with the rule parser only; anything else goes to the lead.

**Prompts out of argv.** The lead's appended system prompt is written to `<home>/lead/append.md` and a lane's memory block to `<home>/lanes/<id>/append.md` (0600, temp file, fsync, rename) right before the child starts; argv holds `--append-system-prompt=<absolute path>`. omp 18.22.2 reads a one-line value as a file once at start (`resolvePromptInput`, `system-prompt.ts`), so later rewrites cannot reach a running child. `CHECKED_OMP_VERSION` covers this fact too. Chat children keep their fixed literal guidance. A lane still runs as the same Unix user and can read these files.

**omp version notice.** `Gateway::with_omp_check` hands the start check to the dispatcher. At the end of `startup`, a warning with no `W_OMP_VERSION` log row for that version sends `N_OMP_VERSION` to the owner's DM, or to the configured channel when the DM fails, and then writes the row. `Gateway::open` prunes `memory_log` and `recall_ledger` rows older than 30 days.

**Old config.** A `[memory.jev]` table (any keys) still loads: `MemoryConfig.ignored` names it, the gateway logs `W_CONFIG_IGNORED`, and `check-config` prints an advisory `WARN memory` line. `to_toml` never writes it back.

### 3.11 레인 작업 폴더 (lanes L1)

**언제 쓰나.** `lane_start`는 선택 인자 `repo`(git 저장소 경로)를 받습니다. 없거나 `null`이면 lane은 지금처럼 lead와 같은 작업 폴더에서 일합니다. 있으면 그 저장소의 lane 전용 git worktree에서 일합니다. `repo`는 절대 경로, `~/`로 시작하는 경로(`HOME` 기준), 작업 폴더 기준 상대 경로 중 하나이고, 풀어서 `canonicalize`한 경로를 `lanes.repo` 열에 저장하고 `lane_status`에도 보여 줍니다. 문자열이 아니거나 비면 `E_LANE_ARGS`, 있는 폴더가 아니면 `E_LANE_REPO`로 거절하고, 거절된 호출은 lane을 저장하지도 자식을 띄우지도 않습니다. 도구 처리기는 git을 부르지 않습니다.

**폴더와 브랜치.** 폴더는 `<작업 폴더>/.moraebot-lanes/lane-<id>`, 브랜치는 `moraebot/lane-<id>`이고 시작점은 그 순간 저장소의 `HEAD`입니다. 작업 폴더 안에 두는 까닭은 첨부 파일의 `.moraebot-inbox/`와 같습니다. 홈 안에 두면 lead의 `send_file`이 그 파일을 보낼 수 없습니다.

**만드는 순서.** lane 워커가 omp 자식을 띄우기 전에 `worktree::ensure`를 부릅니다. git은 셸 없이 argv 그대로, 완료 확인과 같은 환경(`omp::check_env`)에 `GIT_TERMINAL_PROMPT=0`을 더해 돕니다. 출력은 `<홈>/lanes/<id>/git/stdout`, `stderr` 파일로 가고, 명령 하나에 `GIT_TIMEOUT`(120초)이 지나면 그룹째 죽입니다.

1. `git --version`이 0으로 끝나는지 봅니다. 개발 도구가 없는 macOS의 `git` 안내 프로그램처럼 실행은 되지만 실패하는 경우도 여기서 걸립니다.
2. `git -C <repo> rev-parse --show-toplevel --git-common-dir`로 저장소의 맨 위 폴더와 공통 git 폴더를 얻습니다.
3. 폴더가 이미 있으면 새로 만들지 않고 아래의 재사용 확인만 합니다.
4. 없으면 부모 폴더를 만들고 `git -C <맨 위> worktree add -b moraebot/lane-<id> <폴더> HEAD`를 돌립니다.
5. 새 폴더를 재사용 확인과 같은 방법으로 확인하고, 그 worktree 자신의 git 폴더(`--git-dir`)에 빈 표시 파일 `moraebot-ready`를 씁니다.

그다음 lane의 omp는 `--cwd <폴더>`로 뜨고, 완료 확인(3.8)도 그 폴더에서 돕니다. lead의 `--cwd`는 늘 작업 폴더입니다.

**코드.** 실패하면 omp 자식을 띄우지 않고 lead에게 `[moraebot conversation=<key> lane=<id> failed code=<CODE>]`가 가며, lane 상태는 `failed`입니다.

| 코드 | 경우 |
|---|---|
| `E_LANE_GIT` | git을 띄울 수 없음, `git --version`이 0이 아님, git 명령이 `GIT_TIMEOUT`을 넘김 |
| `E_LANE_REPO` | `repo`에서 `rev-parse`가 실패함(git 저장소가 아님) |
| `E_LANE_WORKTREE` | 부모 폴더를 만들 수 없음, `worktree add` 실패, 있던 폴더나 새 폴더가 재사용 확인을 통과하지 못함 |

로그에는 lane id, 순번, 코드만 남기고 경로, 브랜치 이름, git 출력은 남기지 않습니다.

**재사용.** `lane_message`로 일을 더 주면 같은 폴더와 같은 브랜치를 씁니다. 폴더가 있으면 읽기만 하는 `git -C <폴더> rev-parse --show-toplevel --git-dir --git-common-dir --abbrev-ref HEAD`를 돌려, 맨 위 폴더가 그 폴더이고, 브랜치가 `moraebot/lane-<id>`이고, 공통 git 폴더가 `repo`의 것과 같고, `moraebot-ready` 표시 파일이 있을 때만 다시 씁니다. 그냥 폴더, 반쯤 만들어진 worktree(표시 파일 없음), 다른 저장소의 worktree는 쓰지도 지우지도 않고 `E_LANE_WORKTREE`로 실패합니다.

**끄기.** 워커가 돌리는 git 명령은 완료 확인처럼 `LaneSet.checks`에 pid -> `(lane id, 순번)`으로 적힙니다. `shutdown_all`은 lane 자식을 먼저 죽이고, 그다음 git 명령의 그룹을 죽이고 거둘 때까지 기다립니다. 이렇게 끊긴 턴은 결과 줄을 쓰지 않고, 다음 시작 때 `needs_check` / `E_LANE_INTERRUPTED`가 됩니다(3.7). 꺼지는 중이면 git을 시작하지 않고 `E_LANE_STOPPING`으로 끝납니다.

**아직 하지 않는 것.** lane 폴더는 이제 닫을 때 지웁니다(3.13). 브랜치는 지우지 않습니다. `git worktree prune`도, `.git/info/exclude` 변경도 없습니다. 작업 폴더 자체가 git 저장소면 `.moraebot-lanes/`가 그 저장소에서 "추적 안 됨"으로 보입니다. moraebot이 git 폴더 안에 쓰는 파일은 `moraebot-ready` 하나뿐이고, git이 그 worktree를 지울 때 함께 지워집니다.

**dori와 다른 점.** dori에서는 lane(AI)이 직접 worktree를 만들고, dori는 브랜치 이름으로 그 폴더를 찾아 검사하고 닫을 때 지웁니다. moraebot lane은 명령줄 도구가 없는 백그라운드 프로그램이라 moraebot 코드가 만듭니다. 그래서 이름과 위치가 늘 같습니다. lane 이름을 딴 브랜치, 저장소가 없으면 공유 폴더라는 점은 dori와 같습니다. 닫을 때 지우는 방식은 3.13에 있습니다.

### 3.12 레인 완료 git 검사 (lanes L2)

**언제 도나.** `repo`로 시작한 lane(자기 worktree가 있는 lane)이 `lane_done`을 부를 때만 돕니다. `worktree::git_checks`가 lane 폴더의 git 상태를 먼저 읽고, 그다음 완료 확인(3.8)을 지금처럼 돌립니다. git 검사에 걸려도 완료 확인은 돌리므로, 거절 하나에 아직 안 된 것이 모두 실립니다. `repo` 없는 lane은 작업 폴더가 git 저장소여도 git 명령을 하나도 돌리지 않습니다.

**네 명령.** 모두 읽기만 하고, 셸 없이 `done::run_command`로 돌립니다. 환경은 완료 확인과 같고, 여기에 `GIT_TERMINAL_PROMPT=0`과 `GIT_OPTIONAL_LOCKS=0`을 더합니다(`git_env`). 명령 하나에 `GIT_TIMEOUT`(120초)이고, pid는 완료 확인처럼 적혀 끄기 때 함께 죽습니다.

1. `git -C <폴더> rev-parse --show-toplevel`: 맨 위 폴더가 lane 폴더와 같아야 합니다.
2. `git -C <폴더> status --porcelain --untracked-files=no`: 줄이 하나라도 있으면 커밋하지 않은 변경입니다. 스테이지만 한 변경도 셉니다. 새로 만든 파일(추적 안 됨)은 보지 않습니다.
3. `git -C <폴더> remote`: 원격이 하나도 없으면 4번을 건너뛰고 "올림" 검사는 `git pushed (no remote)`로 통과합니다.
4. `git -C <폴더> rev-list --count HEAD --not --remotes`: 0보다 크면 HEAD에 어느 원격 브랜치에도 없는 커밋이 있다는 뜻입니다. 업스트림은 필요 없습니다. 어떤 이름으로든 push하면 컴퓨터의 원격 기록이 바뀌어 0이 됩니다.

moraebot은 `git fetch`도 `git push`도 하지 않습니다. 컴퓨터에 있는 원격 기록만 봅니다.

**코드.** 걸린 검사마다 `E_DONE_UNMET` 거절에 `- <검사> -> <CODE>: <내용>` 한 줄이 붙습니다. 통과한 claim의 영수증에는 git 검사가 먼저 나옵니다.

| 코드 | 검사 | 경우 |
|---|---|---|
| `E_CHECK_UNCOMMITTED` | `git committed` | 추적하는 파일에 커밋하지 않은 변경이 있음 |
| `E_CHECK_UNPUSHED` | `git pushed` | 원격이 있는데 HEAD에 어느 원격에도 없는 커밋이 있음 |
| `E_CHECK_GIT` | `git state` | 네 명령 중 하나가 0으로 끝나지 않음(실행 실패, 시간 초과 포함), 맨 위 폴더가 lane 폴더가 아님, 커밋 수를 읽을 수 없음 |

`E_CHECK_GIT`이면 그 검사 하나만 남고 다른 git 검사는 없습니다. 내용과 로그에는 경로, 브랜치 이름, git 출력을 넣지 않습니다.

**알림.** 완료 줄이 있는 `repo` lane의 안내문에는 완료 안내 뒤에 `worktree::GIT_FOOTER`(커밋하고 브랜치를 올리라는 한 문단)가 붙고, lead 안내문에도 같은 규칙이 한 문장 있습니다.

**dori와 다른 점.** 커밋 안 한 변경과 원격에 없는 커밋을 보고 새 파일은 막지 않는 점, fetch하지 않는 점은 dori와 같습니다. dori는 git 검사에 걸리면 Done 검사를 건너뛰지만 moraebot은 둘 다 돌립니다. dori는 원격이 없는 저장소에서 lane이 끝날 수 없지만 moraebot은 그때 커밋만 봅니다.

### 3.13 레인 닫기 (lanes L2b)

**누가 닫나.** lane을 닫을지는 lead가 정하고, 호스트 도구 `lane_close {lane_id, reason}`으로 닫습니다. moraebot이 스스로 닫는 일은 없습니다(타이머도, 완료 때 자동 닫기도 없음). 다시 여는 도구도 없습니다. `lane_id`가 정수가 아니거나 `reason`이 비면(공백만 있어도) `E_LANE_ARGS`, 모르는 id는 `E_LANE_UNKNOWN`, 게이트웨이가 꺼지는 중이면 `E_LANE_STOPPING`입니다.

**일하는 중이면 거절.** 자식이 돌고 있거나 저장소의 상태가 `running`인 lane은 `E_LANE_BUSY`로 거절하고 아무것도 바꾸지 않습니다. 일하는 lane을 죽이지 않습니다. 결과가 lead에게 돌아온 뒤에 닫으면 됩니다.

**저장.** `lanes` 표에 `closed_at`, `close_reason` 열이 있고, 예전 저장소는 열 때 두 열이 더해집니다. `close_lane`은 한 트랜잭션에서 두 열을 `COALESCE`로 채우므로 다시 닫아도 처음 시각과 이유가 남습니다. 닫힌 lane에 `lane_message`를 보내면 `E_LANE_CLOSED`로 거절합니다. `lane_status`에는 `closed`, `close_reason`으로 계속 보입니다.

**폴더 지우기.** `repo` 없는 lane은 닫기만 하고 git을 하나도 돌리지 않습니다(결과 `worktree: "none"`). `repo` lane은 저장소 잠금(`inner`)을 놓은 뒤 `worktree::remove`를 부릅니다.

1. lane 폴더가 없으면 git을 돌리지 않고 `missing`입니다.
2. L2의 git 검사(3.12)를 그대로 돌립니다. 하나라도 걸리면 폴더를 남깁니다.
3. `git -C <repo> worktree remove <폴더>`를 한 번 돌립니다. `--force`는 쓰지 않으므로 추적 안 된 새 파일이 있으면 git이 스스로 거절합니다. 0으로 끝나면 `removed`, 아니면 `E_LANE_REMOVE`로 남깁니다.

결과는 JSON 한 줄 `{lane_id, closed: true, worktree, branch}`이고, 남겼으면 `codes`와 `detail`이 붙습니다. `worktree`는 `none`, `missing`, `removed`, `kept` 중 하나입니다.

| 코드 | 경우 |
|---|---|
| `E_CHECK_UNCOMMITTED` | 추적하는 파일에 커밋하지 않은 변경이 있음 |
| `E_CHECK_UNPUSHED` | 원격이 있는데 어느 원격에도 없는 커밋이 있음 |
| `E_CHECK_GIT` | git 상태를 읽을 수 없음 |
| `E_LANE_REMOVE` | `git worktree remove`가 실패함(추적 안 된 파일, 잠긴 worktree 등) |
| `E_LANE_STOPPING` | 정리 도중 게이트웨이가 꺼지기 시작함 |
| `E_LANE_WORKTREE` | `<홈>/lanes/<id>/git` 폴더를 준비하지 못함 |

**추적 안 된 파일과 무시된 파일.** 새로 만들고 커밋하지 않은 파일이 있으면 지우면 사라지므로 폴더를 남깁니다(L2의 완료 검사는 이것을 막지 않지만 닫기는 지우는 일이라 더 조심합니다). `.gitignore`에 적힌 파일(빌드 결과물 등)은 폴더와 함께 지워집니다.

**남기는 것.** 브랜치 `moraebot/lane-<id>`는 언제나 남깁니다. 원격이 없는 저장소에서는 커밋이 이 브랜치에 남으므로 잃는 것이 없습니다. `.moraebot-lanes/` 자체, `<홈>/lanes/<id>/`의 세션과 검사 기록도 지우지 않습니다. moraebot은 `git fetch`, `git push`, `git worktree prune`도 하지 않습니다.

**다시 시도.** 폴더를 남긴 lane도 닫힌 상태가 됩니다. 주인이 커밋하거나 올린 뒤 lead가 `lane_close`를 다시 부르면 정리를 다시 해 봅니다.

**끄기.** 닫기의 git 명령도 `LaneSet.checks`에 순번 0으로 적히므로 `shutdown_all`이 함께 죽입니다. 그러면 결과는 `kept`와 `E_LANE_STOPPING`입니다. 로그에는 lane id, `worktree` 값, 코드만 한 줄 남기고 경로, 브랜치, 이유, git 출력은 남기지 않습니다.

**dori와 다른 점.** dori도 닫을 때 `--force` 없는 `git worktree remove`를 쓰고, 지우기가 거절되면 기록만 남기고 닫기는 계속합니다. dori는 자기가 가진 worktree의 git 상태를 읽을 수 없거나 올리지 않은 커밋이 있으면 닫기 자체를 막지만, moraebot은 lane을 닫고 폴더만 남깁니다.

### 3.14 레인 감시 (lanes L3)

**살아 있다는 신호.** lane 자식이 보내는 omp agent 이벤트(`Event::Notification(RpcNotification::RpcAgentEvent(_))`) 하나하나를 그 일의 `Activity`에 셉니다. 짝 없는 응답, 읽지 못한 프레임, agent 이벤트가 아닌 알림은 세지 않습니다. 이벤트를 받는 길에서는 시계를 읽지 않고 숫자만 올립니다.

**진행.** `ToolExecutionStart` 가운데 (도구 이름, 인자) 쌍이 이 일에서 처음 나온 것만 진행으로 셉니다. 같은 호출을 되풀이하면 이벤트는 늘어도 진행은 늘지 않습니다. 마지막 이벤트가 `AutoRetryStart`이면 "재시도 중"으로 표시합니다.

**세 가지 이유.**

| 코드 | 경우 |
|---|---|
| `N_LANE_QUIET` | 20분 동안 agent 이벤트가 하나도 없음 |
| `N_LANE_RETRYING` | 20분 동안 조용하고, 마지막 이벤트가 `AutoRetryStart`(모델 서비스 실패 뒤 재시도) |
| `N_LANE_SPINNING` | 이벤트는 오는데 20분 동안 새 도구 호출이 없음 |

**주기와 기준.** 30초마다(`WATCH_TICK`) 돌고 있는 lane을 하나씩 보고, 기준은 `omp.stall_after_minutes`(`OmpConfig::stall_after`, 빠지면 20분 `DEFAULT_STALL_AFTER`, 0은 거부)입니다. 주기는 상수입니다. 숫자는 dori에서 왔습니다. dori의 `watch`도 30초마다 돌고, 멈춤(hung)과 오래됨(stale) 기준이 둘 다 20분이며, dori도 lane을 죽이지 않습니다. 첫 점검은 시계를 시작하기만 하고, 그 뒤 점검에서 숫자가 바뀌었으면 그때 시각을 적습니다.

**멈춤 한 번에 알림 한 번.** 알린 뒤에는 새 도구 호출이 생겨 그 멈춤이 끝날 때까지 다시 알리지 않습니다. 같은 lane이 다시 20분 멈추면 새 멈춤으로 보고 다시 한 번 알립니다. 알림은 나중에 지우지 않으므로 lead는 "멈춘 것 같다" 다음에 결과를 차례로 읽습니다.

**알림이 가는 길.** lane 결과와 같습니다. 받은편지함에 한 줄(`platform`, `origin`, `author_id`가 모두 `lane`, `message_id`는 `<id>:<seq>:stalled:<episode>`)을 넣고 dispatcher를 깨웁니다. `message_id`가 유일하므로 경합이 있어도 멈춤 하나에 한 줄입니다. lane의 저장 상태는 `running` 그대로입니다. 글은 세 줄입니다.

```text
[moraebot conversation=<key> lane=<id> stalled reason=<N_LANE_...>]
[moraebot] stall quiet_minutes=<n> idle_minutes=<n> tool_calls=<n> new_calls=<n> last_tool=<이름 또는 ->
The lane is still running and moraebot does not stop it; its result comes later as its own input.
```

`lead::parse_tag`가 `stalled`와 `reason=`을 읽고, lead의 기본 역할 글이 이 태그와 세 코드를 설명합니다. 도구 인자, 경로, lane이 쓴 글, lane의 일 내용은 넣지 않습니다. 비밀이 들어 있을 수 있기 때문입니다. 알림을 쓰면 로그에 warn 수준 `lane looks stalled` 한 줄(`code=W_TURN_STALLED`, lane id, 순번, 코드, 멈춤 번호)을 남깁니다.

**lead turn 감시.** 같은 tick에서 dispatcher가 `OmpManager::lead_stalled`로 돌고 있는 lead turn도 봅니다. lead 자식의 이벤트 콜백이 agent 이벤트(글 조각, 도구 호출 시작과 끝 등 무엇이든)를 `LeadWatch`에 세고, tick 시각으로 lane과 같은 방식(첫 점검은 시계만 시작, 숫자가 바뀌면 그 시각 기록)으로 판단합니다. 기준 시간 동안 이벤트가 없으면 그 turn의 대화에 `N_TURN_STALLED` 알림("No progress for 20 minutes. The task may be stuck. It keeps running and nothing was stopped.")을 한 번 보내고 로그에 warn `lead turn looks stalled`(`code=W_TURN_STALLED`)를 남깁니다. 새 이벤트가 오거나 새 turn이 시작되면 다시 알릴 수 있습니다. turn은 죽이거나 다시 켜거나 끊지 않습니다.

**꺼지는 때.** turn 시간 제한(`omp.turn_timeout_minutes`, 빠지면 제한 없음)이 기준 시간 이하이면 시간 제한이 먼저 끝내므로 감시하지 않습니다. 게이트웨이가 꺼지는 중일 때, 그리고 돌고 있는 자식이 없는 lane(쉬는 lane, 끝난 lane, 닫힌 lane)도 보지 않습니다. 감시는 lane을 멈추거나 재촉하거나 상태를 바꾸지 않습니다. 멈추기는 lead가 `lane_stop`으로 합니다(3.16절).

**dori와 다른 점.** dori는 터미널 화면과 파일 변경으로 진행을 보고, 15분 조용하면 lane에 재촉 글을 넣습니다. moraebot은 omp의 이벤트 흐름을 읽고 새 도구 호출을 진행으로 세며, L3에서는 재촉하지 않고 lead에게 알리기만 합니다.

### 3.15 레인 진행 소식 (lanes L4)

**도구와 안내 한 줄.** 모든 lane 일은 `lane_done`(Done 줄이 있을 때) 옆에 `lane_report {text}` 도구를 받습니다. lane 지시문 끝에는 `REPORT_FOOTER` 한 줄이 붙어, 큰 단계마다(목표 하나 끝냄, 막힘) `lane_report`로 짧은 한 줄을 남기라고 알려 줍니다. 처리기는 글을 다듬어 그 일의 가장 새 메모로 두고(이전 메모는 덮어씀) 곧바로 `{"recorded":true}`를 돌려줍니다. 직접 올리지도, 기다리지도 않습니다. 다듬은 글이 비면 `E_LANE_ARGS`로 거절합니다. `lane_report` 호출도 도구 호출이므로 L3 감시에서는 진행으로 셉니다.

**다듬기.** `news_text`가 이 순서로 바꿉니다. `secret::find_secret`이 찾은 키와 비밀번호는 `[secret]`로, 작업 폴더는 `.`로, 홈 폴더는 `~`로 바꿉니다. 경로는 절대 경로일 때만(`/`와 상대 경로, 빈 경로는 건너뜀), 그리고 경로가 끝나는 자리일 때만 바꿉니다. 뒤에 오는 글자가 `/`, 빈칸, 따옴표(`"` `'` `` ` ``)이거나 글 끝이어야 하므로 `/qa/workshop`처럼 이름이 이어지는 경로는 그대로 둡니다. `@`는 전각 `＠`로 바꿔 `@everyone` 같은 호출이 울리지 않게 하고, 줄바꿈과 빈칸 묶음은 빈칸 하나로 합친 뒤 200자(`REPORT_CHARS`)에서 자릅니다.

**메모가 사는 곳.** 메모는 그 일의 L3 `Activity`에 메모리로만 있고, 일이 끝나면 같이 사라집니다. 아직 안 올린 메모도 버립니다. 결과는 원래대로 lead를 거쳐 옵니다. 저장소(DB)에는 쓰지 않으므로 다시 시작하면 아무것도 다시 보내지 않습니다.

**조용함.** 대화마다 말한 횟수를 셉니다. 주인 메시지를 받아들일 때, 답장 글을 보냈을 때, 진행 소식을 lead에게 넘겼을 때 하나씩 오릅니다. 오류 알림 같은 고정 안내문은 세지 않습니다. 횟수를 올릴 때는 시계를 읽지 않고, 점검 때 그 점검 시각으로 "마지막으로 바뀐 때"를 적습니다. 그 시각에서 20분(`NEWS_AFTER`)이 지나면 조용한 대화입니다. 기록이 없는 대화도 조용한 것으로 봅니다. 기록은 커지지 않습니다. 점검마다 조용하고 이번에 올릴 소식이 없는 대화의 기록은 지우고, 다음에 말이 오가면 다시 만듭니다.

**주기와 기준.** L3 감시와 같은 30초 점검(`WATCH_TICK`)을 씁니다. 감시가 한 번 돌 때마다 `watch_ticks`가 그 시각을 알리고, dispatcher가 그 뒤에 소식을 올립니다. 기준 20분은 dori에서 왔습니다. dori도 `postAfterMin` 20분이 지나야 올리고, 가장 새 보고만 올리며, 같은 보고를 되풀이하지 않고, 경로를 지웁니다. 둘 다 상수이고 설정 항목은 없습니다.

**넘기는 길 (#68).** 조용한 대화의 소식은 채팅에 바로 올리지 않고 lead에게 넘기며, lead가 주인의 말로 한 줄 설명합니다(멈춤 알림이 lead를 거치는 #38과 같은 원칙). lead가 그 대화의 turn을 돌리는 중이면 `[moraebot lane news]` 메모(첫 줄 `lane=<id>`, 그 다음 소식)를 #71의 줄 없는 메모 길(`LeadNotes`, `NoteKind::News`)로 그 turn에 넣습니다. 주인의 steer 자리는 쓰지 않습니다. 보낸 메모는 줄에 넣은 것일 뿐이므로, lead가 받았다는 결과(`OnSettled(true)`)가 turn 끝에 와야 다음 점검에서 넘긴 것으로 적고 조용함 시계를 다시 셉니다. omp가 거절했거나 turn이 잘려 답이 없으면(`false`) 다음 점검에서 아래의 받은편지함 줄로 넣어 자기 차례를 기다리게 합니다. 결과가 오기 전에는 같은 소식을 다시 내놓지 않습니다. 그렇지 않으면(lead가 쉬는 중이거나 다른 대화의 turn을 돌리는 중) `Store::news_notice`가 받은편지함에 lane 줄(`message_id`는 `<id>:<seq>:news:<글 해시>`, 글은 `[moraebot conversation=<key> lane=<id> news]` 태그 줄과 소식)을 넣고 dispatcher를 깨웁니다. 한 turn은 자기 대화에만 답하므로 다른 대화의 소식은 이렇게 자기 차례를 기다립니다. lane이 더 이상 돌지 않으면 줄을 넣지 않고, 차례가 왔을 때 lane이 이미 끝났으면 그 줄은 lead에게 가지 않고 `failed`(`E_LANE_NEWS_STALE`)로 버립니다. 결과 줄이 뒤따르기 때문입니다. lead가 시작하지 못하는 동안(`OmpManager::lead_down`, 마지막 시작이 실패하고 그 뒤 성공이 없음)만 예전처럼 `[lane <id>] <text>`를 직접 올립니다. 넘기지 못했으면(저장 오류, 직접 올리기 실패) 다음 점검(30초 뒤)에 다시 시도합니다. 로그에는 글, 대화 키, 경로를 넣지 않습니다.

**같은 글 두 번 없음.** 넘긴 뒤 `news_posted`가 lane별로 글의 해시를 적고, `news`는 그 lane에서 이미 올린 글을 다시 내놓지 않습니다. 해시는 일하는 lane 것만 남기고(`news`가 부를 때마다 나머지를 지움), lane마다 64개(`POSTED_PER_LANE`)까지 둡니다. 가득 차면 비우고 다시 넣습니다. 메모는 언제나 지금 일의 `Activity`에서만 오므로, 지운 해시 때문에 예전 메모가 되살아나지는 않습니다.

**dori와 다른 점.** dori는 상태 메시지 하나를 계속 고치고, 보고를 파일에서 읽습니다. moraebot은 보고를 메모리에 두고 lead를 거쳐 전합니다. 같은 보고의 받은편지함 줄은 `message_id`가 유일해서 한 번만 생깁니다. lane을 재촉하지도 않습니다.

### 3.16 레인 멈추기 (lanes L3b)

**누가 정하나.** lead만 정합니다. moraebot은 스스로 lane을 멈추지 않고, 3.14의 감시도 알리기만 합니다. lead의 기본 역할 글은 이렇게 안내합니다. 주인이 시키면 멈추고, `N_LANE_QUIET`나 `N_LANE_SPINNING` 알림 뒤에는 원래 오래 조용한 작업이 아닌 한 짧은 이유와 함께 `lane_stop`을 부르며, `N_LANE_RETRYING` 뒤에는 결과를 기다립니다.

**도구.** `lane_stop {lane_id, reason}`은 `LaneManager::stop`을 부르고 기다리지 않습니다. 받아들이면 곧바로 `{"lane_id":<id>,"stopping":true}`를 돌려줍니다. 이미 멈추는 중인 일에 다시 부르면 같은 JSON을 돌려주고 아무것도 다시 끄지 않습니다.

**끄는 길.** 게이트웨이가 꺼질 때(`shutdown_all`) 쓰는 길을 lane 일 하나에만 씁니다. 일에 멈춤 표시(이유)를 적고, omp 자식의 프로세스 그룹을 SIGKILL로 끄고, 그 일이 띄운 확인 명령과 git 명령도 모두 끕니다. 그 뒤로 이 일은 새 자식을 띄우지 않고(띄우기 콜백이 거절), 새 확인 명령이나 git 명령도 등록하지 않습니다(`track`이 거절). 이 모든 일은 `inner` 잠금 안에서 합니다.

**RPC `abort`를 쓰지 않는 까닭.** moraebot 어디에서도 `abort`를 쓰지 않고, omp가 응답하지 않으면 결국 강제 종료가 또 필요하기 때문입니다. 이미 쓴 파일은 남고(`repo` lane이면 커밋되지 않은 채 폴더에 남음), 그 turn에서 쓰다 만 내용은 사라질 수 있습니다.

**결과 규칙.** 멈춘 일이 잘려서 끝났으면(결과가 `Err`이거나 `NeedsCheck`, 꺼질 때와 같은 기준) 결과 한 줄을 `failed code=E_LANE_STOPPED`로 쓰고, lane 상태를 `failed`(오류 `E_LANE_STOPPED`)로 두고 dispatcher를 깨웁니다. 끄기 전에 스스로 끝난 일은 자기 결과를 그대로 씁니다. 게이트웨이가 꺼지는 중이면 꺼짐이 이깁니다. 잘린 일은 아무것도 쓰지 않고 다음 시작의 `recover_on_start`에 맡깁니다.

```text
[moraebot conversation=<key> lane=<id> failed code=E_LANE_STOPPED]
[moraebot] Stopped by the lead: <reason>
```

이유는 줄바꿈과 빈칸 묶음을 빈칸 하나로 합친 뒤 200자(`STOP_REASON_CHARS`)에서 자릅니다. 다듬은 이유가 비면 `E_LANE_ARGS`로 거절합니다. 이유는 로그에 남기지 않습니다.

**lane은 열린 채로.** 멈춘 lane은 닫히지 않으므로 `lane_message`로 일을 더 줄 수 있고 `lane_close`도 됩니다. omp의 세션 잠금은 운영체제 잠금이라 꺼진 자식이 끝나면 운영체제가 풀어 주므로, 새 자식이 저장된 세션을 다시 열어 같은 기록을 이어 씁니다.

**거절.** 인자가 틀리면 `E_LANE_ARGS`, 게이트웨이가 꺼지는 중이면 `E_LANE_STOPPING`, 모르는 id는 `E_LANE_UNKNOWN`, 닫힌 lane은 `E_LANE_CLOSED`, 일하지 않는 lane은 `E_LANE_IDLE`입니다.

**로그.** 멈추기를 받아들이면 `lane stop requested` 한 줄(lane id와 순번만)을 남깁니다. 결과는 원래의 `lane finished` 줄이 코드와 함께 남깁니다.

### 3.17 진행 소식 고치기 (lanes L4b)

**메신저 쪽.** 메신저 인터페이스(`MessengerAdapter`)에 `edit(target, message_id, text)`가 있습니다. 기본 구현은 고치기를 못 하는 메신저용이라, 아무 요청도 보내지 않고 `E_ADAPTER_EDIT`로 거절합니다. `send`는 원래부터 올린 메시지의 id를 돌려줍니다(Slack은 `<channel>:<ts>`, Discord는 나눠 보낸 마지막 조각의 id). `edit`는 그 id와, `send`에 준 것과 같은 `target`을 받습니다.

**Slack.** id를 `<channel>:<ts>`로 나눠 `chat.update`에 `{channel, ts, text}`를 보냅니다. 글은 `send`와 똑같이 이스케이프합니다. id 모양이 틀리면 요청을 보내지 않습니다. 거절, 모르는 메시지, 속도 제한은 모두 `E_ADAPTER_EDIT`이고, `edit` 안에서 다시 시도하지 않습니다.

**Discord.** serenity로 `/channels/<target>/messages/<id>`에 `{content}`를 PATCH합니다. target이나 id가 Discord id가 아니면 요청을 보내지 않습니다. 어떤 실패든 `E_ADAPTER_EDIT`입니다.

**#68 이후.** 진행 소식이 lead를 거치게 되면서 dispatcher는 소식 메시지를 고치지 않습니다. lane별 메시지 id 기록(`news_message`, `set_news_message`)과 `deliver_news`는 없어졌고, lead가 시작하지 못할 때의 직접 올리기만 `send`를 씁니다. `edit`는 메신저 인터페이스와 브리지 프로토콜에 그대로 남아 있습니다.

**테스트.** Discord는 127.0.0.1 가짜 서버로 실제 PATCH 요청을 확인합니다. Slack은 다른 Slack 테스트처럼 프로그램 안의 가짜(`FakeSlackTransport`)로 확인하고 네트워크를 쓰지 않습니다.

### 3.18 바쁜 lead와 상태 질문 (#68)

**긴 일은 lane으로.** lead의 기본 역할 글은 몇 분 넘게 걸릴 일(길거나 여러 단계거나 병렬인 일, lane 결과를 오래 검사하는 일 포함)을 자기 turn에서 하지 말고 `lane_start`로 넘긴 뒤, 무엇을 넘겼는지 한 줄로 답하고 turn을 끝내라고 안내합니다.

**바쁜 lead 메모.** 30초 점검마다 dispatcher가 `OmpManager::lead_busy`를 부릅니다. lead turn이 `lead.handoff_after_minutes`(빠지면 5분, 0이면 끔) 넘게 돌았고, 그 turn의 대화에서 주인 줄이 기다리고 있으면(그 turn에 steer로 들어간 줄, 또는 `queued`인 주인 줄. lane 줄은 세지 않음) `[moraebot lead busy]` 메모 하나를 #71의 줄 없는 메모 길로 넣습니다. 남은 일을 lane에 넘기고, 주인에게 답하고, turn을 끝내라는 내용입니다. 주인의 steer 자리는 그대로 비어 있습니다. 다른 대화의 줄은 이 turn이 답할 수 없으므로 세지 않습니다. 메모는 turn마다 한 번이고, 보내면 warn `W_LEAD_BUSY` 한 줄을 남깁니다(`W_TURN_STALLED`와 따로). turn 시간 제한이 기준 이하이면 끕니다. turn을 멈추거나 죽이지 않습니다. 시작 시각은 `LeadWatch::begin`이 적고, 점검은 tick 시각과 비교합니다.

**상태 질문.** `lead.direct_status`(빠지면 `true`)가 켜져 있고 주인 줄이 첨부 없이 상태 질문 하나뿐이면(`status::request`: 앞뒤 빈칸과 끝 문장부호를 떼고 대소문자를 무시한 전체 글이 고정 목록, 예: `뭐 하고 있어`, `뭐해`, `상태`, `status`, `what are you doing`과 같을 때) 메모리 명령 바로 뒤, steer 전에 줄을 가져가 moraebot이 직접 답합니다(`Store::finish_direct`, 메모리 명령과 같은 길). 답은 `LeadWatch`의 turn 대화, 시작 시각, 가장 최근 도구와 그 intent(lane 소식처럼 다듬음), 그리고 돌고 있는 lane에서 만듭니다. lead turn이 이 대화의 것이면 지금 단계까지, 다른 대화의 것이면 바쁘다는 사실만 말합니다. lane도 이 대화의 것만 번호, 시간, 일의 첫 줄을 보이고 다른 대화의 것은 개수만 셉니다. 답 끝에는 두 줄이 붙습니다. lead 프롬프트 캐시 줄(`OmpManager::lead_cache`: 최근 20턴의 캐시 읽기 토큰이 전체 프롬프트 토큰에서 차지하는 비율, 바뀐 기억이 다음 lead 재시작을 기다리면 그 사실)과 기억 떠올리기 줄(모드, 작업 중 모드, 최근 1시간 호출 수, 마지막 전달)입니다. 전체 글이 `기억 새로고침`/`refresh memory`인 주인 줄도 `direct_status`와 상관없이 같은 길로 직접 답하고, lead의 시스템 프롬프트를 다음 턴 전에 다시 정하게 합니다(`Dispatcher::refresh_command`, `OmpManager::refresh_memory`, 3.9절). lead는 이 메시지를 보지 않습니다. 경합이나 재시작으로 확인 없이 큐에 들어간 상태 질문은 worker가 같은 방식으로 답합니다. 목록에 없는 글(`status of PR 12?` 등)과, `direct_status = false`일 때의 상태 질문은 원래대로 lead에게 갑니다. 일의 첫 줄과 도구 intent는 `lane::clean_note`로 다듬습니다. `lane_report`와 같은 경로 목록(lane 뿌리, 작업 폴더, 실제와 정규화한 `$HOME`)을 써서 절대 경로가 `.`와 `~`로 바뀝니다.

### 3.19 New session (#74)

**Why.** Asked for a fresh session, the lead once wrote a script that stopped its own service (the script died with the service's cgroup, so nothing started it again) and cleared the `sessions` row, which changes nothing: `open_session` resumes whatever is in `<home>/lead/session` (3.3). So moraebot owns the switch, and the lead guidance forbids touching the service or session state.

**Matching.** `new_session::request`: the whole owner message, trimmed (whitespace and a closing `.` or `!`), case ignored, equal to `새 세션`, `새 세션 시작`, `새 세션으로 시작`, `new session` or `/new`; `새 세션 확인` / `new session confirm` confirm. It runs first in the owner-command slot, before the memory commands and the status questions. The owner's `/new` passes the `/` filter; anyone else's `/new` gets `N_UNSUPPORTED_INPUT`, and chat senders never reach the slot. The command with attachments gets `N_NEW_SESSION_FILES` before admission (nothing stored or fetched), so the `/` filter and the worker, which only matches rows without attachments, agree.

**Worker only.** On arrival the row is only queued: never steered, never claimed. It waits in the FIFO behind the running turn (moraedang's choice 1 on #74: wait, don't kill), and `Store::steer_into` refuses to steer later rows of its conversation past an older queued row, so they queue behind it. The worker then answers it without the lead (`Store::finish_direct`, like the memory commands).

**Lanes.** With a running lane the answer names their count and asks for the confirmation within `new_session::CONFIRM_WINDOW` (10 minutes); nothing changes. The question is kept in memory per conversation (`session_asked`) and, like the forget question, the owner's next message in that conversation closes it. A confirmation is judged on arrival: one that finds the question open marks its row (`session_confirmed`) for the worker; any other confirmation counts as a plain command and asks again while lanes run. Lanes keep running and their results reach the new lead as ordinary lane rows.

**Switch.** `OmpManager::new_session` takes the `live` mutex, closes the child with `Live::close` (the child is reaped before it returns, so omp no longer writes), renames `<home>/lead/session` to a free `<home>/lead/session-backup-<UTC YYYYMMDDTHHMMSSZ>` (`-2`, `-3`, ... on a collision) and calls `Store::clear_session_id("lead")`, which deletes the row and clears any `conversations.omp_session_id` naming it (nothing writes that column today). `live` stays empty, so the next `prompt` runs `start()` as after any restart: a new empty session folder, no lease check, and a new lead generation (`Store::next_lead_generation`), so recall state of the old child is not reused. The gateway, lanes, chat children, the recall advisor and the memory are untouched. Info `I_LEAD_NEW_SESSION` logs the backup folder. A rename that fails is `E_LEAD_NEW_SESSION`: the owner is told, and the next child resumes the old folder.

## 4. 상태, 설정, 운영

### 4.1 moraebot 홈 폴더

모든 상태는 `~/.moraebot/` 한 곳에 둡니다. 백업하거나 지울 대상이 하나라서 관리가 쉽습니다.

```
~/.moraebot/
  config.toml              설정 파일 (사람이 읽고 고치는 파일)
  bin/moraebot             install-launchd 또는 install-systemd가 복사해 두는 실행 파일 (launchd나 systemd가 이것을 켬)
  bin/moraebot-slack-bridge  Slack 브리지 실행 파일. messenger = "bridge"일 때 같이 복사
  slack-bridge.toml        브리지 설정: 소켓 경로, channel_id, 토큰 파일 경로 (권한 600)
  slack-bridge/            브리지 상태: lock, spool/ (코어가 아직 ack하지 않은 메시지)
  config.toml.pre-bridge   setup slack이 예전 [slack] 설정을 옮길 때 남기는 백업
  secrets/discord.token    Discord 봇 토큰 (주인만 읽기, 권한 600)
  inbox.sqlite             받은 메시지 보관함
  state.sqlite             대화 목록
  lead/session/            lead omp 세션 기록 (--session-dir로 넘김)
  lead/session-backup-*/   old lead sessions moved aside by the new-session command (3.19), never deleted
  lead/append.md           the lead's appended system prompt, 0600 (--append-system-prompt=<path>)
  lanes/<id>/session/      lane마다 omp 세션 기록
  lanes/<id>/append.md     a lane's memory block, 0600
  memory/                  the memory store, a git repository (docs/memory.md)
  memory.head              moraebot's last own memory commit, carried to the next start (docs/memory.md#commits-moraebot-trusts)
  memory.pending           the journal of a memory batch until its commit lands
  omp-overlay.yml          turns omp's own memory off for the lead and lanes
  hints/lead.md            lead 역할 안내문 바꾸기 (선택)
  conversations/           slice 3까지 쓰던 대화별 세션 기록. 더 쓰지 않고 지우지도 않음
  logs/
    gateway.log            게이트웨이 동작 기록
    launchd.out / .err     launchd가 받아 주는 출력과 오류
```

받은 메시지 보관함(`inbox.sqlite`)을 따로 둡니다. dinobot도 보관함을 본 DB와 다른 파일로 두고, 배타 잠금(한 프로그램만 쓰게 막는 잠금)과 `synchronous=FULL`(디스크에 기록된 것을 확인한 뒤 진행)을 씁니다. omp는 원래 대화 기록을 `~/.omp/agent/sessions/`에 저장하지만, `--session-dir`로 위치를 moraebot 홈 안으로 바꿉니다.

### 4.2 설정

| 항목 | 뜻 | 기본값 | 채팅에서 바꾸기 |
|---|---|---|---|
| Discord 토큰 위치 | 토큰 파일 경로. 토큰 자체는 설정 파일에 쓰지 않음 | `secrets/discord.token` | 불가 |
| 접근 모드 | `owner_only`, `allowlist` | `owner_only` | 불가 |
| 주인 id | 주인의 Discord 사용자 id | 없음 (필수) | 불가 |
| 허용 목록 | `allowlist` 모드에서 메신저별로 허용할 id, kind(`user`/`bot`), role(`chat`) | 빈 목록 | 불가 |
| 봇 답장 상한 | `max_bot_replies`: 스레드 하나에서 10분 동안 봇에게 답하는 최대 횟수 | 없음 (제한 없음) | 불가 |
| 작업 폴더 | omp가 일하는 폴더 (`--cwd`) | 없음 (필수) | 불가 (열린 질문 11) |
| 모델 | omp가 쓸 AI 모델 | omp 기본값 | 주인만 가능 (열린 질문 12) |
| 교정 기록 | `[memory.self]`: `enabled`, `correction_patterns` (docs/memory.md#corrections). 옛 `[memory.jev]` 표는 무시하고 `W_CONFIG_IGNORED`로 알림 | 켜짐 | 불가 |
| 기억 떠올리기 | `[memory.recall]`: `mode`(`on`/`shadow`/`off`), `model`, `mid_task`(빠지면 `mode`를 따름), `mid_max_delivered`(2), `mid_cooldown_minutes`(10), `wait_ms`(2500) 등 (docs/memory.md#settings). 옛 `mid_max_wakes_per_turn`, `mid_min_interval_seconds`는 무시하고 `W_CONFIG_IGNORED`로 알림 | `on`, 모델 `@smol` | 불가 |
| lead 프롬프트 캐시 | `[lead] cache_minutes`: 마지막 턴 뒤 이만큼 쉬기 전에는 시스템 프롬프트를 그대로 두고 바뀐 기억을 갱신 블록으로 넣음. 0이면 바뀔 때마다 lead를 다시 켬 (3.9절) | 60분 | 불가 |

규칙은 하나입니다. **봇을 빼앗을 수 있는 항목은 파일에서만 바꿉니다.**

토큰과 주인 id가 그런 항목입니다. 채팅으로 주인 id를 바꿀 수 있다면, 주인 계정이 한 번만 털려도 봇을 영영 빼앗깁니다.

접근 모드와 허용 목록도 같은 이유로 파일에서만 바꿉니다. 허용 목록을 넓히는 것은 봇을 남에게 여는 일이기 때문입니다.

주인 id가 비어 있으면 게이트웨이는 시작하지 않습니다.

### 4.3 계속 켜 두기 (macOS, Linux)

launchd는 macOS가 프로그램을 자동으로 켜고, 죽으면 다시 켜 주는 기능입니다. `~/Library/LaunchAgents/`에 설정 파일(plist) 하나를 두고 `KeepAlive`와 `RunAtLoad`를 켜면, 로그인할 때 시작하고 죽으면 다시 띄웁니다. 이 키의 정확한 동작은 **미검증**입니다. 사용자 로그인 단위(LaunchAgent)로 돌려야 omp가 주인의 홈 폴더와 인증 정보를 쓸 수 있다고 보지만, 근거를 확인하지 못해 **미검증**입니다.

Linux에서는 `moraebot install-systemd`가 systemd 사용자 서비스 파일 `~/.config/systemd/user/moraebot.service`를 씁니다(`XDG_CONFIG_HOME`이 절대 경로면 그 아래 `systemd/user`). 그다음 `systemctl --user daemon-reload`, `enable`, `restart`를 차례로 실행합니다. 서비스 파일에는 `Restart=always`와 `RestartSec=10`(꺼지면 10초 뒤 다시 켜기), `TimeoutStopSec=30`(맥의 `ExitTimeOut`과 같은 값), `WantedBy=default.target`(로그인하면 켜기)이 들어 있습니다. 사용자 서비스는 로그아웃하면 멈추므로, 로그아웃 뒤에도 돌게 하려면 주인이 `loginctl enable-linger`를 직접 실행합니다. moraebot은 이 명령을 안내만 하고 실행하지 않습니다. 설정 키의 뜻은 systemd 설명서 [systemd.unit(5)](https://www.freedesktop.org/software/systemd/man/latest/systemd.unit.html)와 [systemd.exec(5)](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html)를 따릅니다. 실제 Linux 컴퓨터에서의 동작은 **미검증**입니다.

**Slack 브리지 서비스.** `messenger = "bridge"`이면 `install-systemd`는 유닛 두 개를 설치합니다. `moraebot-slack-bridge.service`(같은 `Restart=always`, `RestartSec=10`, `TimeoutStopSec=30`, `UMask=0077`)와 `moraebot.service`입니다. 코어 유닛은 브리지 유닛을 `Wants=`와 `After=`로 가리킵니다. `Requires`가 아니라서 브리지를 멈춰도 코어는 멈추지 않습니다. 명령은 `daemon-reload` 뒤에 브리지, 그다음 코어 순서로 `enable`과 `restart`를 실행합니다. 브리지가 아닌 설정이면 예전에 남은 브리지 유닛을 끄고 지웁니다. `install-launchd`도 같은 방식으로 에이전트 두 개(`<label>`, `<label>.slack-bridge`)를 설치합니다. launchd에는 순서가 없으므로 코어의 기다림과 재접속이 순서를 대신합니다. 소켓 경로는 `setup`이 한 번 정해 두 설정 파일에 같은 절대 경로로 씁니다. macOS에는 `$XDG_RUNTIME_DIR`이 없어서 `~/Library/Application Support/moraebot/slack-bridge.sock`을 풀어 쓴 경로가 들어갑니다. `uninstall-*`은 둘 다 지웁니다.

### 4.4 재시작했을 때

```
 launchd --(죽으면 다시 실행)--> moraebot 게이트웨이
   1. inbox.sqlite 열기
   2. processing으로 남은 메시지 처리 (아래 설명)
   3. 남아 있는 옛 omp 자식 프로세스 종료하고 거두기
   4. Discord Gateway 다시 연결
   5. 메시지가 오면 lead omp를 다시 띄우고 open_session으로 lead 대화 이어 받기
   6. 대기(queued) 메시지를 순서대로 처리 (같은 메시지 id는 한 번만)
```

- **2번**: dinobot은 `processing`을 `queued`로 되돌려 다시 처리합니다. 하지만 moraebot은 yolo라서 같은 명령이 두 번 실행되면 위험합니다. 그래서 자동으로 다시 보내지 않고, 주인에게 "재시작으로 중단되었습니다. 다시 보낼까요?"라고 묻기를 권장합니다(열린 질문 3).
- **3번**: 옛 자식이 세션을 쥐고 있으면 새 자식의 기록이 조용히 갈라집니다(3.5절). 그래서 옛 자식을 반드시 먼저 끝냅니다.
- **끊긴 작업**: 재시작하면 omp가 실행하던 도구 작업은 이어지지 않습니다. 대화 기록은 남지만, 하던 명령은 끊깁니다.

## 5. 단계별 로드맵

한 번에 하나씩, 앞 단계가 실제로 동작한 뒤에 다음으로 갑니다(D8). 단계 하나는 처음부터 끝까지 실제로 동작하는 작은 기능 단위입니다.

1. **1단계: 왕복 한 번.** Discord 메시지 -> 게이트웨이 -> inbox 저장 -> omp 대화 하나 -> `prompt_result` -> Discord 답장. 주인 한 명(`owner_only`), 대화 하나, 설정 파일만 씁니다. launchd 등록과 재시작 처리(4.4절)를 포함합니다. 토큰은 권한 600(주인만 읽을 수 있음) 파일에서만 읽고, 로그와 자식 프로세스 환경변수에 넣지 않습니다. 메시지 본문은 셸(명령 실행기)을 거치지 않고 JSON 한 줄로만 omp에 보냅니다. 대화는 Discord 스레드 하나입니다. 가짜 Discord와 가짜 omp로 자동 테스트하고, 실제 Discord에서 한 번 수동으로 확인합니다.
2. **2단계: 접근 모드.** `owner_only`와 `allowlist`(주인 + chat 역할). 받을 때와 보내기 직전에 두 번 검사합니다. `pairing`과 `open`은 빠졌습니다(2.6절).
3. **3단계: 여러 대화.** Discord 스레드마다 omp 하나, 대화별 큐. 바쁠 때 `followUp`과 `steer`, `/stop`과 `/new`.
4. **4단계: 일 나누기(나중).** dori-mode는 예전에 쓰던, 큰 일을 여러 에이전트에게 나눠 맡기던 방식입니다. 그 아이디어를 옮깁니다.
   - 주인 채널에 "리드 대화" 하나를 둡니다.
   - 일 하나씩을 맡는 추가 대화(레인)를 게이트웨이가 띄웁니다.
   - 레인이 "끝났다"고 하면, 게이트웨이가 완료 조건을 직접 확인한 뒤 주인에게 보고합니다.
   - 오래 소식이 없으면 재촉하는 감시는 게이트웨이 안의 타이머로 합니다.
   - dori-mode와 달리 화면을 읽지 않고 omp RPC의 결과를 씁니다.

   자세한 설계는 이 단계에 들어갈 때 합니다.
5. **light (v0.3.0).** 0.2.0의 기억 장치를 걷어내고 첫 만남과 `persona.md` 하나만 남깁니다(3.9절). omp 기억은 주인 설정을 따릅니다(3.10절). 기억 코드는 `memory-full` 브랜치에 그대로 두어 나중에 다시 합칠 수 있습니다.
6. **lane 기능.** 한 번에 하나씩, 앞 것이 동작한 뒤에 다음으로 갑니다.
   - **L1 레인 작업 폴더.** `repo`를 받은 lane은 자기 git worktree에서 일합니다(3.11절).
   - **L2 올리지 않은 작업 검사 (완료).** `lane_done` 때 lane 폴더에 커밋하지 않은 변경이나 원격에 올리지 않은 커밋이 있으면 거절합니다(3.12절).
   - **L2b 레인 닫기와 폴더 정리 (완료).** lead가 lane을 닫으면 L2 검사를 거친 뒤 lane 폴더를 지웁니다. 브랜치는 남깁니다(3.13절).
   - **L3 레인 감시 (완료, 3.14절).** 멈추거나 헛도는 lane을 시간 제한 전에 lead에게 한 번 알립니다. lane을 끄지는 않습니다.
   - **L3b 레인 멈추기 (완료, 3.16절).** lead가 일하는 lane의 지금 일을 끊습니다. lane은 열린 채로 남습니다.
   - **L4 진행 소식 (완료, 3.15절).** lane이 일하는 동안 짧은 진행 소식을 메신저로 보냅니다. L3의 주기 점검을 씁니다.
   - **L4b 진행 소식 고치기 (완료, 3.17절).** lane마다 진행 소식 메시지 하나를 고칩니다(Slack chat.update, Discord 메시지 수정). 고치지 못하면 새로 올립니다.
   - **Done 확인 종류 더하기 (완료, 3.8절).** `url`, `command ... contains`, `file ... contains`를 더해 확인 방법이 다섯 가지가 됩니다.
   - **GitHub PR 합침 확인 (완료, 3.8절).** `merged <주인>/<저장소>#<번호>`를 더해 확인 방법이 여섯 가지가 됩니다. `closed`, `published`는 아직 받지 않습니다.

## 6. 열린 질문

각 질문에 권장 기본값을 적었습니다. 따로 정하지 않으면 권장 기본값으로 갑니다.

1. **홈 폴더 위치.** 권장: `~/.moraebot/`.
2. **대화 단위를 스레드로 강제할까?** 권장: 채널에 바로 쓴 메시지는 봇이 스레드를 자동으로 만들고, 그 스레드를 대화 키로 씁니다.
3. **재시작이나 이상 종료 뒤 끊긴 메시지는?** `processing`으로 남은 메시지, omp가 종료 코드 1로 끝난 메시지가 여기에 해당합니다. 권장: 자동으로 다시 보내지 않고 "확인 필요"로 표시한 뒤 주인에게 다시 보낼지 묻습니다. yolo라서 같은 명령이 두 번 실행되면 위험합니다.
4. **처리 중에 온 새 메시지는 `followUp`인가 `steer`인가?** 권장: `followUp`. 끼어들기(`steer`)는 사용자가 명시할 때만 씁니다.
5. **게이트웨이가 꺼져 있던 동안 온 메시지를 찾아 읽을까?** 권장: 처음에는 하지 않습니다. 켜질 때 재시작 알림만 보내고, 놓친 메시지는 다시 보내 달라고 안내합니다.
6. **주인 id는 어디에 적을까?** 권장: 설정 파일에 Discord 사용자 id 하나. 비어 있으면 게이트웨이가 시작하지 않습니다.
7. **어떤 omp-rpc 커밋에 고정할까?** 권장: 설치된 omp 18.20.2와 같은 태그의 커밋. omp를 올릴 때 함께 올립니다.
8. **진행 상황을 얼마나 보여 줄까?** 권장: 처음에는 "입력 중" 표시만. 도구 실행 요약은 다음 단계에서.
9. **쉬는 대화의 omp는 언제 끌까?** 권장: 30분 동안 메시지가 없으면 stdin을 닫아 정상 종료합니다. 다음 메시지가 오면 다시 띄웁니다.
10. **공식 Rust 클라이언트를 쓸까, 직접 짤까?** 권장: 공식 클라이언트를 전용 스레드에서 씁니다. 문제가 생기면 tokio용 얇은 클라이언트를 직접 짭니다.
11. **작업 폴더를 채팅에서 바꿀 수 있게 할까?** 권장: 3단계까지는 설정 파일에서만. 바꿀 수 있으면 에이전트가 다른 폴더를 마음대로 건드리게 됩니다.
12. **모델을 채팅에서 바꿀까?** 권장: 주인만, 대화 단위로. 2단계 이후.
13. **로그 보관 기간.** 권장: 하루 단위로 나누고 14일 보관.
14. **`open` 모드를 아예 허용할까?** 결정: 허용하지 않습니다. 모드는 `owner_only`와 `allowlist`만 둡니다(2.6절).

## 7. 근거 (개발자용)

코드 위치는 각 프로젝트의 공개 저장소 기준입니다. `wavesN/...` 표기는 공개 저장소에 포함하지 않는 로컬 조사 메모입니다.

**Discord와 게이트웨이**
- Discord Gateway와 `message_content` intent(hermes): `ext/hermes-agent/repo/plugins/platforms/discord/adapter.py:1423-1430`, `:1488` (waves4/hermes-agent.md:25)
- 게이트웨이 한 프로세스가 항상 켜짐(hermes): waves4/hermes-agent.md:9, :33
- dinobot Discord는 interactions 웹훅만, 일반 메시지 안 받음: `discord.ts:74` (waves4/dinobot.md:48)
- serenity 0.12.5, twilight-gateway 0.18.0 버전: waves5/verify.md:129. 재연결과 resume 처리 주장: waves5/rust.md:43 (범위 미검증, rust.md:86)
- Discord Gateway는 영구 재전송 없음, 메시지 id로 중복 제거: waves5/rust.md:92
- rusqlite 0.43.0, `Transaction`, `TransactionBehavior::Immediate`: waves5/verify.md:79, :119. 단일 쓰기 권장: waves5/rust.md:54

**저장, 순서, 제어 명령**
- 대화 키 구성(hermes): `gateway/session.py:722-766` (waves4/hermes-agent.md:69)
- 메시지, 중복 키, 커서를 한 트랜잭션에: `messaging.ts:623-651`. 중복 방지 검사: `messaging.ts:630`, UNIQUE 제약: `messaging.ts:684`. 재시작 때 `processing`을 `queued`로: `messaging.ts:713` (waves4/verify.md:52, :54; waves4/dinobot.md:59-61, :104-107)
- dinobot 메시지 상태 6개(queued, processing, ready, sending, done, dead): `messaging.ts:144` (waves4/dinobot.md:78)
- inbox 별도 파일, 배타 잠금, `synchronous=FULL`: waves4/dinobot.md:78, waves4/verify.md:52
- dinobot의 Discord와 Slack 전역 큐(동시 1개, Telegram은 토픽별)와 head-of-line blocking: `messaging.ts:104`, `:240` (waves4/dinobot.md:86, :133)
- `/new`가 옛 대화를 `<키>#n`으로 보관: `threads.ts:431-478` (waves4/dinobot.md:83)
- `/stop`, `/new` 큐 우회(hermes): `gateway/platforms/base.py:3946-3950` (waves4/verify.md:11)
- dinobot은 launchd 아래에서 자체 재시작을 하지 않음: waves4/dinobot.md:111

**접근 제어**
- 두 모드(`owner_only`, `allowlist`)는 moraebot 설계. hermes에서 가져온 것은 기본 거부와 허용 목록이고, pairing은 참고만 했습니다. 기본 거부와 allowlist 순서: `gateway/authz_mixin.py:858-862, 905-935`. pairing 규칙: `gateway/pairing.py:1-32`. 그룹은 pairing 안 함: `run_inbound.py:228-247` (waves4/verify.md:12; waves4/hermes-agent.md:58, :75, :97)
- 받을 때와 보낼 때 두 번 검사(dinobot): `authorizeStored`, `messaging.ts:379` (waves4/verify.md:53)

**omp RPC**
- 한 줄에 JSON 하나(JSONL), JSON-RPC 2.0 아님: `oh-my-pi/docs/rpc.md:1-9` (waves5/verify.md §1)
- `ready` 내용: `rpc.md:41-52`. 버전 2와 `rpc_chunk`: `rpc.md:54-75`
- stdin을 닫으면 처리 후 코드 0, 저장 실패 시 코드 1: `rpc.md:34`, `:1679`
- 출력이 밀리면 임시 파일, stdout 계속 읽기: `rpc.md:79`, `:81`
- `id` 응답과 순서 무관: `rpc.md:111-123`, `:233-238`(bash 명령 예시). `prompt`는 앞선 명령 뒤에 시작: `rpc.md:343`
- `prompt_result`와 status, 실패 턴도 성공 응답: `rpc.md:339-369`. `agentInvoked:false`면 `prompt_result` 없음: `rpc.md:355-357`. `sessionSettled`: `rpc.md:358`, `:365`, `:373`. 답장 글은 이벤트 또는 `assistant_text`: `sdk/rust/omp-rpc/README.md:29`
- `open_session`과 `resumed`, `--continue`와 같음: `rpc.md:381-405` (waves5/omp-rpc.md:100-107, waves5/verify.md:23, :101)
- 스트리밍 중 `streamingBehavior` 필수, steer와 followUp 전달 시점: `rpc.md:1264-1275`. steering의 `interruptMode` 설명(abort와 다름): `rpc.md:1277-1298`. abort 결과 `status: "aborted"`: `rpc.md:363`. `abort_and_restore_queue`: `rpc.md:447-461`
- 같은 세션(세션 id 기준 임대)에 두 프로세스면 조용히 기록 분기: `docs/session.md:558-559` (waves5/verify.md:40, :137)
- 기본 승인 모드 yolo, 제한 폴더 예외: `docs/approval-mode.md:25`, `:50`, `:63`
- 실행 명령 예시와 `--cwd`, `--approval-mode`: waves5/omp-rpc.md:137
- 세션 기본 위치 `~/.omp/agent/sessions/`: waves5/omp-rpc.md:102. 한 프로세스에 세션 하나는 추론: waves5/omp-rpc.md:101
- Rust 클라이언트 구조, 블로킹 전송, v2 자동 협상: `sdk/rust/omp-rpc/README.md:5-15`, `:37`. 쓰기 스레드와 종료 절차: `README.md:43-59`. bash 작업 생존: `README.md:61`. `prompt_and_wait`: `README.md:67`. README 예시는 `--no-session`: `README.md:22`
- `publish = false`와 crates.io 이름 충돌: waves5/verify.md R4. `spawn_blocking` 필요: waves5/verify.md R5

**dori-mode (4단계용)**
- 리드, 레인, 완료 검증, 모니터, 부팅 점검 흐름: waves3/verify.md:105-116. 상태 값과 자동 닫힘: waves3/verify.md:31, :85, :88. 화면 읽기 방식: waves3/verify.md:89

**미검증**
- launchd plist 키(`KeepAlive`, `RunAtLoad`)의 정확한 동작
- LaunchAgent로 돌려야 omp가 홈 폴더와 인증 정보를 쓴다는 주장
- Message Content intent가 꺼졌을 때 DM과 멘션 메시지의 본문 동작
- Discord 라이브러리의 재연결과 resume 범위, 재연결 후 놓친 메시지 처리
- Discord "입력 중" 표시 API와 유지 시간
