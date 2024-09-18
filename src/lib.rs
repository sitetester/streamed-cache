use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use std::{
    collections::HashMap,
    result::Result,
    sync::{Arc, Mutex},
};

type User = String;
type CommentsCount = u64;

#[async_trait]
pub trait Api: Send + Sync + 'static {
    async fn fetch(&self) -> Result<HashMap<User, CommentsCount>, String>;
    async fn subscribe(&self) -> BoxStream<Result<(User, CommentsCount), String>>;
}

#[derive(Clone)]
pub struct StreamCache {
    results: Arc<Mutex<HashMap<User, CommentsCount>>>,
}

// Its goal is to always return the most recent user comments count
impl StreamCache {
    pub async fn new(api: impl Api) -> Self {
        let instance = Self {
            results: Arc::new(Mutex::new(HashMap::new())),
        };

        let api = Arc::new(api);

        // get data from `subscribe()`
        {
            let instance = instance.clone();
            let api = api.clone();
            tokio::spawn(async move {
                let mut sub = api.subscribe().await;
                while let Some(Ok((user, comments_count))) = sub.next().await {
                    let mut map = instance.results.lock().unwrap();
                    map.insert(user, comments_count);
                }
            });
        }

        // get data from `fetch()`, but ignore the ones already retrieved from `subscribe()`
        {
            let fetch_map = api.fetch().await.unwrap();
            let mut map = instance.results.lock().unwrap();
            for (user, comments_count) in fetch_map {
                if map.contains_key(&user) {
                    continue;
                }
                map.insert(user, comments_count);
            }
        }

        instance
    }

    pub fn get(&self, user: &str) -> Option<CommentsCount> {
        let results = self.results.lock().expect("poisoned");
        results.get(user).copied()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio::time;

    use futures::{future, stream::select, FutureExt, StreamExt};
    use maplit::hashmap;

    use super::*;

    #[derive(Default)]
    struct TestApi {
        signal: Arc<Notify>,
    }

    #[async_trait]
    impl Api for TestApi {
        async fn fetch(&self) -> Result<HashMap<User, CommentsCount>, String> {
            // fetch is slow and may get delayed until after we receive the first updates
            self.signal.notified().await;
            Ok(hashmap! {
                "Alex123".to_string() => 10,
                "John456".to_string() => 15,
            })
        }

        async fn subscribe(&self) -> BoxStream<Result<(User, CommentsCount), String>> {
            let results = vec![
                Ok(("John456".to_string(), 20)),
                Ok(("Jamie789".to_string(), 30)),
            ];
            select(
                futures::stream::iter(results),
                async {
                    self.signal.notify_one();
                    future::pending().await
                }
                    .into_stream(),
            )
                .boxed()
        }
    }

    #[tokio::test]
    async fn works() {
        let cache = StreamCache::new(TestApi::default()).await;

        // Allow cache to update
        time::sleep(Duration::from_millis(100)).await;

        assert_eq!(cache.get("Alex123"), Some(10));
        assert_eq!(cache.get("John456"), Some(20));
        assert_eq!(cache.get("Jamie789"), Some(30));

        assert_eq!(cache.get("Dummy"), None);

        // we assume cache stored total 3 entries at this stage
        let results = cache.results.lock().unwrap();
        assert_eq!(results.len(), 3);
    }
}
