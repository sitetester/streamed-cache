use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use std::{
    collections::HashMap,
    result::Result,
    sync::{Arc, Mutex},
};

type City = String;
type Temperature = u64;

#[async_trait]
pub trait Api: Send + Sync + 'static {
    async fn fetch(&self) -> Result<HashMap<City, Temperature>, String>;
    async fn subscribe(&self) -> BoxStream<Result<(City, Temperature), String>>;
}

#[derive(Clone)]
pub struct StreamCache {
    results: Arc<Mutex<HashMap<City, Temperature>>>,
}

// Its goal is to always return the most recent temperature
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
                while let Some(Ok((city, temperature))) = sub.next().await {
                    let mut map = instance.results.lock().unwrap();
                    map.insert(city, temperature);
                }
            });
        }

        // get data from `fetch()`, but ignore the ones already retrieved from `subscribe()`
        {
            let fetch_map = api.fetch().await.unwrap();
            let mut map = instance.results.lock().unwrap();
            for (city, temperature) in fetch_map {
                if map.contains_key(&city) {
                    continue;
                }
                map.insert(city, temperature);
            }
        }

        instance
    }

    pub fn get(&self, city: &str) -> Option<Temperature> {
        let results = self.results.lock().expect("poisoned");
        results.get(city).copied()
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
    struct WeatherApi {
        signal: Arc<Notify>,
    }

    #[async_trait]
    impl Api for WeatherApi {
        async fn fetch(&self) -> Result<HashMap<City, Temperature>, String> {
            // fetch is slow and may get delayed until after we receive the first updates
            self.signal.notified().await;
            Ok(hashmap! {
                "Berlin".to_string() => 20,
                "London".to_string() => 30,
            })
        }

        async fn subscribe(&self) -> BoxStream<Result<(City, Temperature), String>> {
            let results = vec![
                Ok(("Berlin".to_string(), 25)),
                Ok(("Paris".to_string(), 35)),
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
        let cache = StreamCache::new(WeatherApi::default()).await;

        // Allow cache to update
        time::sleep(Duration::from_millis(100)).await;

        assert_eq!(cache.get("Berlin"), Some(25));
        assert_eq!(cache.get("London"), Some(30));
        assert_eq!(cache.get("Paris"), Some(35));

        assert_eq!(cache.get("NewYork"), None);
    }
}
