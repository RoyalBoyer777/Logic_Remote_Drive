"""
Logitech G29 方向盘数据采集与 MQTT 遥控程序
=============================================

功能概述
--------
1. 通过 pygame 读取 Logitech G29 方向盘的轴 / 按钮 / 方向帽数据；
2. 将方向盘数据换算为遥控控制信号（速度 speed、转向 steer、档位 shift、刹车 brake），
   通过 MQTT 发布到处于"遥控驾驶"状态的 AMR 车辆；
3. 车辆的运行模式（自动驾驶 / 遥控驾驶）以车体端实际反馈为准：本程序订阅
   /AMR1/control_status 与 /AMR2/control_status（车体端反馈的整数 0/1/2，受上位机
   runtype_set 控制），据此判断遥控目标车；自身不发布 runtype_set。

版本说明
--------
V2.5（当前版本）
  - 不再监听 runtype_set，改为监听车体端实际模式反馈：
      订阅 /AMR1/control_status 与 /AMR2/control_status；
      载荷为纯整数（非 JSON 字典）：0=另一台遥控器控制（忽略），
      1=自动驾驶，2=本程序遥控驾驶。
  - 值语义与 run_type 约定一致，仅数据来源由上位机指令改为车体反馈。

V2.4
  - 遥控控制话题改名：/AMR1/remote_control、/AMR2/remote_control
    改为 /AMR1/remote_control_wqgk、/AMR2/remote_control_wqgk（载荷内容不变）；
  - 同步更新接收端 mqtt_receiver_test.py 的订阅话题。

V2.3
  - 增加 MQTT 收发检测与打印：
      * 发布 remote_control 时打印话题与载荷，并检查 publish 排队结果（rc）；
      * 通过 on_publish 回调确认 Broker 已收到发布（QoS>=1 时返回 PUBACK）；
      * 收到 runtype_set 时打印原始数据；
      * 连接异常断开 / 重连时打印提示。
  - 仅日志与检测增强，不改变遥控驾驶触发与发布逻辑。

V2.2
  - 明确 run_type 上位机协议：
      0 = 另一台遥控器控制（与本程序无关，忽略，不改变状态）；
      1 = 自动驾驶；
      2 = 本程序执行遥控驾驶控制（向对应车发布 remote_control）。
  - 修正 V2.1 的 run_type 解析约定（原"0=遥控驾驶、1/2=自动驾驶"）。

V2.1
  - 设备无档位硬件，改用离合踏板代替档位：
      离合未踩下（clutch_scaled 在死区 CLUTCH_DEADZONE 内）→ 前进档 gear=1；
      离合踩下（超过死区）→ 后退档 gear=3；
  - 不再通过档位设置 P 档（gear=2），也不再强制驻车制动（brake_scaled=1），
    刹车完全跟随刹车踏板；
  - 内部遥控状态值统一为 2=遥控驾驶、0=自动驾驶（修复状态值不一致导致的发布失效）；
  - 刹车判定增加死区 BRAKE_DEADZONE，避免踏板噪声误发 brake:true。

V2.0
  - 运行模式改由上位机发布：订阅 /AMR1/runtype_set 与 /AMR2/runtype_set，
    解析 run_type（0=遥控驾驶，1=自动驾驶）确定哪辆车处于遥控驾驶状态；
  - 向处于遥控驾驶状态的车辆发布对应 remote_control 话题：
      AMR1 遥控 -> /AMR1/remote_control
      AMR2 遥控 -> /AMR2/remote_control
  - 两辆车均处于自动驾驶（或尚未收到 runtype_set，状态未知）时不发布任何话题；
  - 本程序不再发布 runtype_set（由上位机负责）；
  - 移除 V1.1 的上位机目标车回传机制（/remote/amr_select 及其解析逻辑）。

V1.1
  - 新增订阅上位机回传话题 /remote/amr_select，由上位机界面选择遥控目标车
    （AMR1 或 AMR2）；
  - 仅当方向盘按钮 23 切换的遥控驾驶标志位生效时，向所选目标车发布
    runtype_set 与 remote_control。

V1.0
  - 初始版本：固定向 /AMR1/runtype_set 与 /AMR1/remote_control 发布数据；
    遥控驾驶标志位由方向盘按钮 23（红圈按键）切换。

话题约定
--------
| 方向 | 话题                    | 发布方   | 内容                                   |
|------|-------------------------|----------|----------------------------------------|
| 订阅 | /AMR1/control_status    | 车体端   | 整数 0|1|2（0=其他遥控器 1=自动 2=本程序遥控） |
| 订阅 | /AMR2/control_status    | 车体端   | 同上                                    |
| 发布 | /AMR1/remote_control_wqgk  | 本程序   | {"speed","steer","shift","brake"}      |
| 发布 | /AMR2/remote_control_wqgk  | 本程序   | 同上（内容一致，仅话题名不同）          |

control_status 约定：0=另一台遥控器控制（本程序忽略），1=自动驾驶，2=本程序遥控驾驶。

用法
----
    python logic_control2.py [--index 0] [--interval 0.5] [--count 0]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass

import paho.mqtt.client as mqtt

# 隐藏 pygame 启动时默认输出的欢迎信息。
os.environ.setdefault("PYGAME_HIDE_SUPPORT_PROMPT", "1")

import pygame


# 让标准输出按行实时刷新，避免终端里只显示第一行。
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True, write_through=True)


# ============================================================
# MQTT 配置（根据实际环境修改以下参数）
# ============================================================
# Broker 地址说明：
#   - broker.emqx.io（公共测试服务器，无需安装即可使用）
#   - localhost（需在本机安装 Mosquitto：https://mosquitto.org/download/）
#   - 局域网 IP（连接到内网其他机器上的 Broker）
MQTT_BROKER = "139.129.19.53"        # MQTT 代理服务器公网IP
#MQTT_BROKER = "broker.emqx.io"  # MQTT 测试使用
MQTT_PORT = 1883                 # MQTT 端口号（默认 1883，TLS 通常用 8883）
MQTT_CLIENT_ID = "wqgk_g29_publisher"  # 客户端 ID，同一 Broker 下需唯一
MQTT_USERNAME = "wqgk"           # 用户名，None 表示匿名连接
MQTT_PASSWORD = "wqgk"           # 密码，None 表示匿名连接
MQTT_QOS = 1                     # 服务质量 QoS：0-最多一次 1-至少一次 2-恰好一次
MQTT_KEEPALIVE = 60              # 心跳保活间隔，单位秒
# ============================================================


# ============================================================
# 遥控参数（根据实际设备调整）
# ============================================================
# 设备无档位硬件，使用离合踏板代替档位：
#   clutch_scaled 为离合踏板缩放值（0=未踩下，1000=踩到底）。
CLUTCH_DEADZONE = 100            # 离合死区：缩放值超过该值视为踩下离合（后退档），默认 100≈10% 行程
# brake_scaled 为刹车踏板缩放值（0=未踩下，1000=踩到底）。
BRAKE_DEADZONE = 50              # 刹车死区：缩放值超过该值才视为踩下刹车，避免噪声误报
# ============================================================


@dataclass
class G29Data:
    """保存一帧方向盘和踏板数据。"""

    seq: int
    timestamp_ms: int
    wheel: float
    throttle: float
    brake: float
    clutch: float
    wheel_scaled: int
    throttle_scaled: int
    brake_scaled: int
    clutch_scaled: int
    hat_x: int
    hat_y: int
    button_mask: int
    buttons: list[int]
    axes: list[float]
    remote_driving_flag: int = 0
    gear: int = 0


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="使用 pygame 读取 Logitech G29 方向盘数据。")
    parser.add_argument("--index", type=int, default=0, help="手柄设备索引，默认 0")
    parser.add_argument("--interval", type=float, default=0.5, help="打印周期，单位秒，默认 0.5")
    parser.add_argument("--count", type=int, default=0, help="循环次数，默认 0 表示持续运行")
    return parser.parse_args()


def clamp(value: float, lower: float, upper: float) -> float:
    """限制数值范围。"""
    return max(lower, min(upper, value))


def scale_wheel(axis_value: float) -> int:
    """将方向盘轴值从 -1~1 映射到 -1000~1000。"""
    return int(round(clamp(axis_value, -1.0, 1.0) * 1000))


def scale_pedal(axis_value: float) -> int:
    """将踏板轴值从 -1~1 映射到 0~1000。

    G29 在 pygame 下常见表现为：
    - 松开踏板时接近 +1
    - 踩到底时接近 -1
    """
    normalized = (1.0 - clamp(axis_value, -1.0, 1.0)) / 2.0
    return int(round(normalized * 1000))


def build_button_mask(buttons: list[int]) -> int:
    """将按下的按钮编号压缩为位图，方便 PLC 侧解析。"""
    mask = 0
    for button in buttons:
        if 0 <= button < 32:
            mask |= 1 << button
    return mask


def map_speed(throttle_scaled: int) -> float:
    """将油门缩放值（0~1000）映射为速度值（0.0~5.0）。"""
    return round(throttle_scaled / 200.0, 2)


def map_steer(wheel_scaled: int) -> float:
    """将方向盘缩放值（-1000~1000）映射为转向值（-27.0~27.0）。"""
    return round(wheel_scaled * 27.0 / 1000.0, 2)


def map_brake(brake_scaled: int, deadzone: int = BRAKE_DEADZONE) -> bool:
    """将刹车缩放值转换为布尔量：超过死区阈值才视为踩下刹车。

    避免 G29 踏板松开时残留的微小噪声（缩放值 1~3）误报 brake:true。
    """
    return brake_scaled > deadzone


def map_gear_from_clutch(
    clutch_scaled: int,
    deadzone: int = CLUTCH_DEADZONE,
) -> int:
    """根据离合踏板缩放值确定档位（离合代替档位硬件）。

    离合未踩下（缩放值在死区 CLUTCH_DEADZONE 内）→ 前进档 gear=1；
    离合踩下（缩放值超过死区）→ 后退档 gear=3。
    """
    return 3 if clutch_scaled > deadzone else 1


def read_g29_data(joystick: pygame.joystick.Joystick, seq: int) -> G29Data:
    """读取一帧 G29 数据。"""
    pygame.event.pump()

    axes = [round(joystick.get_axis(i), 6) for i in range(joystick.get_numaxes())]
    buttons = [i for i in range(joystick.get_numbuttons()) if joystick.get_button(i)]
    hats = [joystick.get_hat(i) for i in range(joystick.get_numhats())]

    # 约定前 4 个轴分别对应：方向盘、油门、刹车、离合。
    wheel = axes[0] if len(axes) > 0 else 0.0
    throttle = axes[1] if len(axes) > 1 else 0.0
    brake = axes[2] if len(axes) > 2 else 0.0
    clutch = axes[3] if len(axes) > 3 else 0.0

    hat_x, hat_y = hats[0] if hats else (0, 0)

    return G29Data(
        seq=seq,
        timestamp_ms=int(time.time() * 1000),
        wheel=wheel,
        throttle=throttle,
        brake=brake,
        clutch=clutch,
        wheel_scaled=scale_wheel(wheel),
        throttle_scaled=scale_pedal(throttle),
        brake_scaled=scale_pedal(brake),
        clutch_scaled=scale_pedal(clutch),
        hat_x=hat_x,
        hat_y=hat_y,
        button_mask=build_button_mask(buttons),
        buttons=buttons,
        axes=axes,
    )


def format_console_output(data: G29Data) -> str:
    """生成终端中文输出。"""
    return (
        f"第 {data.seq:04d} 次采样 | "
        f"方向盘原始值：{data.wheel:+.6f} | "
        f"方向盘缩放值：{data.wheel_scaled:+d} | "
        f"油门原始值：{data.throttle:+.6f} | "
        f"油门缩放值：{data.throttle_scaled} | "
        f"刹车原始值：{data.brake:+.6f} | "
        f"刹车缩放值：{data.brake_scaled} | "
        f"离合原始值：{data.clutch:+.6f} | "
        f"离合缩放值：{data.clutch_scaled} | "
        f"按下按钮：{data.buttons if data.buttons else '无'} | "
        f"按钮位图：{data.button_mask} | "
        f"方向帽：({data.hat_x}, {data.hat_y}) | "
        f"遥控驾驶标志：{data.remote_driving_flag} | "
        f"档位：{data.gear}"
    )


def build_remote_control_payload(data: G29Data) -> str:
    """构建 /AMR1/remote_control_wqgk 话题的 JSON 负载。

    包含速度（speed）、转向（steer）、档位（shift）、刹车（brake）。
    """
    payload = {
        "speed": map_speed(data.throttle_scaled),
        "steer": map_steer(data.wheel_scaled),
        "shift": data.gear,
        "brake": map_brake(data.brake_scaled),
    }
    return json.dumps(payload, ensure_ascii=False)


def build_amr_topics(amr_id: int) -> tuple[str, str]:
    """根据目标车编号生成 MQTT 话题。

    返回 (control_status 话题, remote_control 话题)：
        AMR1 -> ("/AMR1/control_status", "/AMR1/remote_control_wqgk")
        AMR2 -> ("/AMR2/control_status", "/AMR2/remote_control_wqgk")
    control_status 由车体端反馈（纯整数 0/1/2）、本程序订阅；
    remote_control_wqgk 由本程序发布。两者内容随目标车不同。
    """
    amr_id = 1 if amr_id not in (1, 2) else amr_id
    return (
        f"/AMR{amr_id}/control_status",
        f"/AMR{amr_id}/remote_control_wqgk",
    )


# 每辆车的遥控驾驶状态：2=遥控驾驶，0=自动驾驶，-1=未知（尚未收到 control_status）。
# 由 on_message 回调（MQTT 后台线程）更新，主循环读取。
_amr_remote_state: dict[int, int] = {1: -1, 2: -1}

# 已排队发布的 (mid -> (话题, 载荷))，供 on_publish 回调确认 Broker 已接收。
_publish_log: dict[int, tuple[str, str]] = {}


def on_connect(
    client: mqtt.Client,
    userdata: None,
    flags: dict,
    reason_code: mqtt.ReasonCode,
    properties: mqtt.Properties | None = None,
) -> None:
    """连接成功回调：订阅两辆车的车体反馈话题（control_status，车体端反馈整数 0/1/2）。"""
    if reason_code == 0:
        for amr_id in (1, 2):
            status_topic, _ = build_amr_topics(amr_id)
            client.subscribe(status_topic, qos=MQTT_QOS)
        print(
            f"[MQTT] 连接成功（收发链路已建立），已订阅："
            f"{build_amr_topics(1)[0]}、{build_amr_topics(2)[0]}（车体端反馈）",
            flush=True,
        )
    else:
        print(f"[MQTT] 连接异常，reason_code={reason_code}，将自动重连。", flush=True)


def on_disconnect(
    client: mqtt.Client,
    userdata: None,
    disconnect_flags: dict,
    reason_code: mqtt.ReasonCode,
    properties: mqtt.Properties | None = None,
) -> None:
    """断开连接回调：打印断开原因（0=主动断开，非 0=异常断开，将自动重连）。"""
    if reason_code != 0:
        print(f"[MQTT] 连接异常断开，reason_code={reason_code}，将自动重连…", flush=True)
        # 丢弃断线前未确认的发布记录。
        _publish_log.clear()


def on_publish(
    client: mqtt.Client,
    userdata: None,
    mid: int,
    reason_code: mqtt.ReasonCode,
    properties: mqtt.Properties | None = None,
) -> None:
    """发布确认回调：Broker 已确认收到该消息（QoS>=1 时返回 PUBACK）。"""
    topic, payload = _publish_log.pop(mid, ("?", "?"))
    if reason_code == 0:
        print(f"[MQTT] [OK] Broker 已确认收到发布 mid={mid} -> {topic}", flush=True)
    else:
        print(
            f"[MQTT] [ERR] Broker 发布确认异常 mid={mid} -> {topic} reason_code={reason_code}",
            flush=True,
        )


def on_message(
    client: mqtt.Client,
    userdata: None,
    msg: mqtt.MQTTMessage,
) -> None:
    """接收消息回调：解析车体反馈 control_status，更新对应车辆是否处于遥控驾驶状态。

    control_status 为纯整数（车体端实际模式反馈，受上位机 runtype_set 控制）：
        0 = 另一台遥控器控制，与本程序无关（忽略，不改变状态）；
        1 = 自动驾驶；
        2 = 本程序执行遥控驾驶控制。
    """
    global _amr_remote_state

    # 判断消息属于哪辆车的 control_status 话题。
    amr_id = None
    for candidate in (1, 2):
        status_topic, _ = build_amr_topics(candidate)
        if msg.topic == status_topic:
            amr_id = candidate
            break
    if amr_id is None:
        return

    # control_status 载荷为纯整数（如 b"2"），不是 JSON 字典。
    try:
        status = int(msg.payload.decode("utf-8").strip())
    except (ValueError, UnicodeDecodeError, TypeError):
        print(f"control_status 解析失败：{msg.topic} -> {msg.payload}", flush=True)
        return

    # control_status 约定：
    #   0 = 另一台遥控器控制（与本程序无关，忽略，不改变状态）
    #   1 = 自动驾驶
    #   2 = 本程序遥控驾驶
    # 非法值同样忽略，保持原状态。
    print(f"[MQTT] 收到 <- {msg.topic}: {status}", flush=True)

    if status == 0:
        return
    if status not in (1, 2):
        return

    # status=2 遥控驾驶 -> remote_state=2；status=1 自动驾驶 -> remote_state=0
    remote_state = 2 if status == 2 else 0
    if _amr_remote_state[amr_id] != remote_state:
        _amr_remote_state[amr_id] = remote_state
        print(
            f"*** AMR{amr_id} 运行模式变化："
            f"{'遥控驾驶' if remote_state == 2 else '自动驾驶'}（话题：{msg.topic}）***",
            flush=True,
        )


def main() -> int:
    """程序主入口：读取 G29 数据，并按上位机设定的遥控目标车发布控制信号。"""
    args = parse_args()

    print("正在启动 G29 数据监视程序...", flush=True)

    # 初始化 pygame 和 joystick 子模块。
    pygame.init()
    pygame.joystick.init()

    joystick_count = pygame.joystick.get_count()
    if joystick_count <= args.index:
        raise SystemExit(f"未找到索引为 {args.index} 的设备，当前识别到的手柄数量为 {joystick_count}。")

    joystick = pygame.joystick.Joystick(args.index)
    joystick.init()

    print(f"已连接设备：{joystick.get_name()}", flush=True)
    print(
        f"轴数量：{joystick.get_numaxes()}，按钮数量：{joystick.get_numbuttons()}，方向帽数量：{joystick.get_numhats()}",
        flush=True,
    )

    # 初始化 MQTT 客户端并连接 Broker。
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
    if MQTT_USERNAME is not None:
        mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_publish = on_publish
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=30)
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
        mqtt_client.loop_start()  # 启动后台网络线程
        print(
            f"MQTT 已连接：{MQTT_BROKER}:{MQTT_PORT}，"
            f"订阅话题：{build_amr_topics(1)[0]}、{build_amr_topics(2)[0]}；"
            f"发布话题：/AMR1 或 /AMR2 的 remote_control_wqgk",
            flush=True,
        )
    except Exception as exc:
        print(f"MQTT 连接失败：{exc}，程序将继续运行但不发送 MQTT 数据。", flush=True)
        mqtt_client = None  # 标记为不可用

    print("开始读取方向盘与踏板数据，按 Ctrl+C 停止。", flush=True)
    print(
        "MQTT 收发检测提示：本程序订阅车体反馈 /AMR1/control_status、"
        "/AMR2/control_status（整数 0/1/2）；\n"
        "  车体反馈为 2（本程序遥控驾驶）时向对应车发布 remote_control_wqgk，"
        "并在终端打印每条收发记录以确认链路正常。",
        flush=True,
    )

    seq = 0
    remote_driving_flag = 0
    prev_button_23 = False
    try:
        while True:
            data = read_g29_data(joystick, seq)

            # 按钮 23 切换本地遥控驾驶标志位（仅用于终端显示，便于观察）。
            # 注意：遥控驾驶模式以车体反馈 control_status 为准，本标志不再参与 MQTT 发布。
            button_23_pressed = 23 in data.buttons
            if button_23_pressed and not prev_button_23:
                remote_driving_flag = 1 - remote_driving_flag
                print(f"*** 按钮23按下，遥控驾驶标志位切换为：{remote_driving_flag} ***", flush=True)
            prev_button_23 = button_23_pressed
            data.remote_driving_flag = remote_driving_flag

            # 使用离合踏板代替档位硬件（无档位硬件）：
            #   离合未踩下（缩放值在死区 CLUTCH_DEADZONE 内）→ 前进档 gear=1；
            #   离合踩下（超过死区）→ 后退档 gear=3。
            # 不再通过档位设置 P 档（gear=2），刹车完全跟随刹车踏板。
            data.gear = map_gear_from_clutch(data.clutch_scaled)

            # 打印本地中文说明，便于调试。
            print(format_console_output(data), flush=True)

            # 依据车体反馈 control_status 判断遥控目标：向处于遥控驾驶状态（反馈为 2）
            # 的车辆发布 remote_control_wqgk；两辆车都处于自动驾驶（或状态未知）时不发布。
            if mqtt_client is not None:
                remote_targets = [
                    amr_id for amr_id in (1, 2)
                    if _amr_remote_state.get(amr_id) == 2
                ]
                if remote_targets:
                    try:
                        remote_control_payload = build_remote_control_payload(data)
                        for amr_id in remote_targets:
                            _, remote_control_topic = build_amr_topics(amr_id)
                            msg_info = mqtt_client.publish(
                                remote_control_topic, remote_control_payload, qos=MQTT_QOS
                            )
                            if msg_info.rc != mqtt.MQTT_ERR_SUCCESS:
                                print(
                                    f"[MQTT] [ERR] 发布排队失败！topic={remote_control_topic} "
                                    f"rc={msg_info.rc}",
                                    flush=True,
                                )
                            else:
                                _publish_log[msg_info.mid] = (
                                    remote_control_topic,
                                    remote_control_payload,
                                )
                                print(
                                    f"[MQTT] 发布 -> {remote_control_topic}: "
                                    f"{remote_control_payload} (mid={msg_info.mid})",
                                    flush=True,
                                )
                    except Exception as exc:
                        print(f"MQTT 发布失败：{exc}", flush=True)

            seq += 1
            if args.count > 0 and seq >= args.count:
                break

            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("用户已停止读取。", flush=True)
    finally:
        if mqtt_client is not None:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
            print("MQTT 已断开。", flush=True)
        joystick.quit()
        pygame.joystick.quit()
        pygame.quit()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
