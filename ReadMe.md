# Apache Superset dataset api 测试工具

## 使用方式
* auth.py 配置文件（目前支持的测试api类型：dataset or dashboard）
* probar.py 主文件

## 关联issue：[#33857](https://github.com/apache/superset/issues/33828)
### 🐞 Bug Fix Log: Filter Parameter Contamination in Concurrent Requests

#### ✅ Bug Reproduction Steps:

1. Trigger an API call (e.g., `/api/v1/dataset/`) from the frontend;
2. Observe that the response contains incorrect or inconsistent data;
3. The request format is correct, and the issue is consistently reproducible.

---

#### 🔍 Investigation & Root Cause Analysis:

1. **API Implementation Review:**

   * The bug pointed to an API endpoint implemented via Superset’s Flask App Builder (FAB) default method (not overridden);
   * No method was explicitly defined for the given HTTP method, implying the use of FAB’s default handling.

2. **Cache Hypothesis Eliminated:**

   * Disabled all forms of caching across Superset (dataset, explore, etc.);
   * Bug persisted — cache was ruled out as the source of the issue.

3. **Parameter Parsing Issue Suspected:**

   * Focused on the `_handle_filters_args` method used to process Rison-encoded filter arguments;
   * Discovered that the `filters` instance was being stored as a class instance variable (`self._filters`);
   * In a concurrent (asynchronous) request environment, this shared state is **not thread-safe**;
   * Different coroutines accessing and mutating the same `filters` object led to **state contamination**;
   * This resulted in responses with incorrect filter criteria being applied, though **no errors were logged** at runtime.

---

#### 🛠 Resolution Strategy:

* **Goal:** Ensure each request processes its own isolated `Filters` object to eliminate cross-request contamination;
* **Old behavior:** The instance reused `self._filters`, clearing it between requests (e.g., via `.clear()`);
* **New behavior:** Allocate a **new `Filters` instance per request**, fully decoupled from class/shared state.

#### ✅ Key Fix (Code Snippet):

```python
def _handle_filters_args(self, rison_args: Dict[str, Any]) -> Filters:
    """
    Handle filters arguments passed to the API endpoint.
    Parses and applies filtering criteria provided as Rison-encoded arguments
    to construct a Filters instance. This method ensures that each request 
    uses an isolated Filters instance to avoid shared state issues 
    in concurrent or asynchronous environments.
    """
    filters = self.datamodel.get_filters(
        search_columns=self.search_columns,
        search_filters=self.search_filters
    )
    filters.rest_add_filters(rison_args.get(API_FILTERS_RIS_KEY, []))
    return filters.get_joined_filters(self._base_filters)
```

---

#### 📌 Summary:

* The root cause was shared mutable state (`self._filters`) in concurrent request handling;
* In asynchronous environments, instance variables must not be reused across requests without protection;
* This fix introduces request-level isolation of `Filters` objects to ensure correct and predictable behavior;
* No functional regression expected, and filter behavior is now stable under concurrent load.


