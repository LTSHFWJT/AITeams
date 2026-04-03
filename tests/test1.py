import sqlite3

from deepagents import create_deep_agent
from deepagents.backends import StoreBackend
from langgraph.store.sqlite import SqliteStore

from aiteams.utils import make_uuid7

# 1. 初始化 SQLite 存储
conn = sqlite3.connect("memory.db", check_same_thread=False, isolation_level=None)
store = SqliteStore(conn)
store.setup()

# 2. 为每个 create_deep_agent 对象分配一个 uuidv7
agent_id = make_uuid7()
namespace = ("agent_id", agent_id)


# 3. 配置 StoreBackend 以使用特定命名空间
def backend_factory(rt):
    return StoreBackend(rt, namespace=lambda _ctx: namespace)


# 4. 创建 Agent
agent = create_deep_agent(
    backend=backend_factory,
    store=store,
)
