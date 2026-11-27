//! The reference point and what follows from it (city, district, postal code, the NINA region),
//! resolved once and stored, so that a start never depends on the geocoder.
//!
//! The home is configured as an address, a point (`[filter] lat`, `lon`), or both. With a point
//! the profile is known at once and the details (city, district, postal code) are resolved in
//! the background; with an address only, the stored profile is used when it was resolved for
//! this address, else one Nominatim search resolves it. The task that keeps the profile
//! publishes it on a `watch` channel: the warnings start at once and pick it up when it is
//! there, the loops that measure from the point start once it is known. A failed NINA region
//! lookup leaves the region unknown and is tried again; a changed address keeps the old profile
//! until the new one is resolved. The profile is resolved again when it is 30 days old (checked
//! daily).

use std::time::Duration;

use mxbot_common::supervise::Alive;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::{config::FilterConfig, fetch::Http, geo::NOMINATIM_TIMEOUT, store::Db, url_encode};

/// Where the bot measures from.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HomeProfile {
    /// The configured address it was resolved for ("" with a configured point only).
    pub(crate) address: String,
    pub(crate) lat: f64,
    pub(crate) lon: f64,
    pub(crate) city: Option<String>,
    /// Bezirk, or the city district / suburb where a city has no Bezirke.
    pub(crate) borough: Option<String>,
    pub(crate) postcode: Option<String>,
    pub(crate) country_code: Option<String>,
    /// NINA's region key (AGS); `None` outside Germany, where NINA does not know the city, or
    /// while the lookup has not worked.
    pub(crate) nina_ags: Option<String>,
    /// When the details were resolved; 0 while they never were (a configured point).
    pub(crate) resolved_at: i64,
}

impl HomeProfile {
    pub(crate) fn point(&self) -> (f64, f64) {
        (self.lat, self.lon)
    }

    /// A German city whose NINA region is not known (yet).
    fn lacks_nina_region(&self) -> bool {
        self.country_code.as_deref() == Some("de") && self.city.is_some() && self.nina_ags.is_none()
    }
}

/// The configured home: an address, a point, or both.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HomeConfig {
    pub(crate) address: Option<String>,
    pub(crate) point: Option<(f64, f64)>,
}

impl HomeConfig {
    /// The home of `[filter]`; `None` when neither an address nor a point is configured.
    pub(crate) fn of(filter: &FilterConfig) -> Option<Self> {
        let point = filter.lat.zip(filter.lon);
        let address = filter.reference_address.clone();
        (address.is_some() || point.is_some()).then_some(HomeConfig { address, point })
    }

    fn address(&self) -> &str {
        self.address.as_deref().unwrap_or("")
    }

    /// Whether `stored` was resolved for this home.
    fn matches(&self, stored: &HomeProfile) -> bool {
        stored.address == self.address() && self.point.is_none_or(|point| point == stored.point())
    }

    /// The profile known without a request: the configured point, with the details `stored`
    /// has for the same address.
    fn known_point(&self, stored: Option<&HomeProfile>) -> Option<HomeProfile> {
        let (lat, lon) = self.point?;
        let same = stored.filter(|s| s.address == self.address());
        Some(HomeProfile {
            address: self.address().to_owned(),
            lat,
            lon,
            city: same.and_then(|s| s.city.clone()),
            borough: same.and_then(|s| s.borough.clone()),
            postcode: same.and_then(|s| s.postcode.clone()),
            country_code: same.and_then(|s| s.country_code.clone()),
            nina_ags: same.and_then(|s| s.nina_ags.clone()),
            resolved_at: same.map_or(0, |s| s.resolved_at),
        })
    }
}

/// How the first resolution is repeated: the first wait, doubling up to the second.
pub(crate) const RETRY: (Duration, Duration) =
    (Duration::from_secs(60), Duration::from_secs(10 * 60));
/// How old a profile may get before it is resolved again.
pub(crate) const REFRESH_EVERY: Duration = Duration::from_secs(30 * 24 * 60 * 60);
/// How often the task looks whether a refresh is due.
const CHECK_EVERY: Duration = Duration::from_secs(24 * 60 * 60);

/// The profile for `address`: the stored one when it was resolved for this address (no
/// request), else resolved now and stored.
pub(crate) async fn home_profile(
    db: &Db,
    http: &Http,
    address: &str,
) -> Result<HomeProfile, String> {
    match db.home_profile().await {
        Ok(Some(stored)) if stored.address == address => return Ok(stored),
        Ok(_) => {}
        Err(e) => warn!("Could not read the stored home profile: {e:#}"),
    }
    let profile = resolve(http, address).await?;
    if let Err(e) = db.store_home_profile(&profile).await {
        warn!("Could not store the home profile: {e:#}");
    }
    Ok(profile)
}

/// The profile of `home` for a single run (the dry run): a configured point needs no request.
pub(crate) async fn home_profile_once(
    db: &Db,
    http: &Http,
    home: &HomeConfig,
) -> Result<HomeProfile, String> {
    let stored = db.home_profile().await.ok().flatten();
    if let Some(stored) = stored.as_ref().filter(|s| home.matches(s)) {
        return Ok(stored.clone());
    }
    match (home.known_point(stored.as_ref()), &home.address) {
        (Some(profile), _) => Ok(profile),
        (None, Some(address)) => home_profile(db, http, address).await,
        (None, None) => Err("no reference address or point".to_owned()),
    }
}

/// One Nominatim search for `address`, then the NINA region of a German city (a failed region
/// lookup leaves it unknown).
pub(crate) async fn resolve(http: &Http, address: &str) -> Result<HomeProfile, String> {
    let mut profile = search(http, address).await?;
    if let Err(e) = add_nina_region(http, &mut profile).await {
        warn!("NINA region not known yet: {e}");
    }
    Ok(profile)
}

/// The NINA region of a German city's profile; `Err` when the lookup failed.
async fn add_nina_region(http: &Http, profile: &mut HomeProfile) -> Result<(), String> {
    if let (Some("de"), Some(city)) = (profile.country_code.as_deref(), profile.city.as_deref()) {
        profile.nina_ags = crate::alerts::nina_ags_for_city(http, city).await?;
    }
    Ok(())
}

/// The details of `home`: a search for its address, else the place at its point; a configured
/// point stays the point.
async fn geocode(http: &Http, home: &HomeConfig) -> Result<HomeProfile, String> {
    let mut profile = match (&home.address, home.point) {
        (Some(address), _) => search(http, address).await?,
        (None, Some((lat, lon))) => {
            let url = format!(
                "https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json&addressdetails=1"
            );
            let hit = nominatim(http, &url).await?;
            from_hit(&hit, "")?
        }
        (None, None) => return Err("no reference address or point".to_owned()),
    };
    if let Some((lat, lon)) = home.point {
        (profile.lat, profile.lon) = (lat, lon);
    }
    Ok(profile)
}

/// One Nominatim search for `address`.
async fn search(http: &Http, address: &str) -> Result<HomeProfile, String> {
    let url = format!(
        "https://nominatim.openstreetmap.org/search?q={}&format=json&addressdetails=1&limit=1",
        url_encode(address)
    );
    let json = nominatim(http, &url).await?;
    let hit = json
        .as_array()
        .and_then(|hits| hits.first())
        .ok_or_else(|| format!("the geocoder does not know {address:?}"))?;
    from_hit(hit, address)
}

async fn nominatim(http: &Http, url: &str) -> Result<serde_json::Value, String> {
    let response = http
        .get_api(url, &[("Accept-Language", "de")], NOMINATIM_TIMEOUT)
        .await
        .map_err(|e| format!("geocoder: {e}"))?;
    if !response.is_success() {
        return Err(format!("geocoder answered HTTP {}", response.status));
    }
    serde_json::from_str(&response.body).map_err(|e| format!("geocoder answer: {e}"))
}

/// A profile from one Nominatim result (search or reverse), without the NINA region.
fn from_hit(hit: &serde_json::Value, address: &str) -> Result<HomeProfile, String> {
    let coordinate = |field: &str| {
        hit[field]
            .as_str()
            .and_then(|v| v.parse::<f64>().ok())
            .ok_or_else(|| format!("geocoder answer without {field}"))
    };
    let details = &hit["address"];
    let first = |fields: &[&str]| {
        fields
            .iter()
            .find_map(|field| details[*field].as_str())
            .map(str::to_owned)
    };
    Ok(HomeProfile {
        address: address.to_owned(),
        lat: coordinate("lat")?,
        lon: coordinate("lon")?,
        borough: first(&["borough", "city_district", "suburb"]),
        postcode: first(&["postcode"]),
        city: first(&["city", "town", "village", "municipality"]),
        country_code: first(&["country_code"]),
        nina_ags: None,
        resolved_at: chrono::Utc::now().timestamp(),
    })
}

/// Keep the profile of `home` and publish it on `profile` for the bot's lifetime: the one
/// known without a request at once, then resolved until it works (`retry`: the first wait and
/// the longest), the NINA region of a German city looked up until the lookup answers, and the
/// whole resolved again when it is [`REFRESH_EVERY`] old. Reported on `alive`: failed while
/// there is no profile or a changed address is not resolved yet, idle while the bot runs on a
/// point or a profile whose refresh failed, ok when it is complete.
pub(crate) async fn keep_profile(
    db: Db,
    http: Http,
    home: HomeConfig,
    alive: Alive,
    retry: (Duration, Duration),
    profile: watch::Sender<Option<HomeProfile>>,
) {
    let stored = match db.home_profile().await {
        Ok(stored) => stored,
        Err(e) => {
            warn!("Could not read the stored home profile: {e:#}");
            None
        }
    };
    let mut current = match stored {
        Some(stored) if home.matches(&stored) => Some(stored),
        stored => home.known_point(stored.as_ref()).or(stored),
    };
    if let Some(known) = &current {
        log_profile(known);
        profile.send_replace(Some(known.clone()));
    }
    // A German city without a region is looked up again once per start.
    let mut nina_pending = current.as_ref().is_some_and(HomeProfile::lacks_nina_region);
    let (mut wait, longest) = retry;
    loop {
        let now = chrono::Utc::now().timestamp();
        let geocode_due = current.as_ref().is_none_or(|p| {
            p.address != home.address()
                || p.resolved_at == 0
                || now - p.resolved_at > REFRESH_EVERY.as_secs() as i64
        });
        let outcome = if geocode_due {
            match geocode(&http, &home).await {
                Ok(mut fresh) => {
                    nina_pending = add_nina_region(&http, &mut fresh).await.is_err();
                    store_and_publish(&db, &profile, &fresh).await;
                    current = Some(fresh);
                    Ok(())
                }
                Err(e) => Err(e),
            }
        } else if nina_pending {
            let mut known = current.clone().expect("a profile");
            match add_nina_region(&http, &mut known).await {
                Ok(()) => {
                    nina_pending = false;
                    store_and_publish(&db, &profile, &known).await;
                    current = Some(known);
                    Ok(())
                }
                Err(e) => Err(format!("NINA region: {e}")),
            }
        } else {
            Ok(())
        };
        match (outcome, &current) {
            (Ok(()), _) if !nina_pending => {
                alive.ok();
                wait = retry.0;
                tokio::time::sleep(CHECK_EVERY).await;
                continue;
            }
            (Ok(()), _) => alive.idle(),
            (Err(e), None) => {
                warn!(
                    "Reference address not resolved ({e}); distances and the forecast wait, next try in {}s",
                    wait.as_secs()
                );
                alive.failed(&e);
            }
            (Err(e), Some(known)) if known.address != home.address() => {
                warn!(
                    "The changed reference address is not resolved yet ({e}), keeping the old one"
                );
                alive.failed("address changed, not resolved yet");
            }
            (Err(e), Some(_)) => {
                warn!("Resolving the home profile failed, keeping the one there is: {e}");
                alive.idle();
            }
        }
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(longest);
    }
}

async fn store_and_publish(
    db: &Db,
    profile: &watch::Sender<Option<HomeProfile>>,
    fresh: &HomeProfile,
) {
    if let Err(e) = db.store_home_profile(fresh).await {
        warn!("Could not store the home profile: {e:#}");
    }
    log_profile(fresh);
    profile.send_if_modified(|known| {
        let changed = known.as_ref() != Some(fresh);
        *known = Some(fresh.clone());
        changed
    });
}

fn log_profile(profile: &HomeProfile) {
    info!(
        "Reference point: {:.5}, {:.5}; district {}; NINA region {}",
        profile.lat,
        profile.lon,
        profile.borough.as_deref().unwrap_or("unknown"),
        profile.nina_ags.as_deref().unwrap_or("none")
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::fetch::Recorded;
    use crate::testing::{fixture, test_db};

    const ADDRESS: &str = "Yourstreet 1, 10000 Yourcity";
    const SEARCH: &str = "https://nominatim.openstreetmap.org/search?q=Yourstreet+1";

    fn stored() -> HomeProfile {
        HomeProfile {
            address: ADDRESS.to_owned(),
            lat: 52.4736,
            lon: 13.4205,
            city: Some("Yourcity".to_owned()),
            borough: Some("YourDistrict".to_owned()),
            postcode: Some("10000".to_owned()),
            country_code: Some("xx".to_owned()),
            nina_ags: None,
            resolved_at: 1,
        }
    }

    #[tokio::test]
    async fn a_stored_profile_for_the_same_address_needs_no_request() {
        let (db, _dir) = test_db();
        db.store_home_profile(&stored()).await.unwrap();
        let recorded = Arc::new(Recorded::new());

        let profile = home_profile(&db, &Http::recorded(recorded.clone()), ADDRESS)
            .await
            .unwrap();

        assert_eq!(profile, stored());
        assert!(recorded.calls().is_empty(), "{:?}", recorded.calls());
    }

    #[tokio::test]
    async fn one_search_resolves_point_district_and_postcode() {
        let (db, _dir) = test_db();
        let mut old = stored();
        old.address = "Oldstreet 9, 10000 Yourcity".to_owned();
        db.store_home_profile(&old).await.unwrap();
        let recorded =
            Arc::new(Recorded::new().route(SEARCH, 200, &fixture("nominatim/reference.json")));

        let profile = home_profile(&db, &Http::recorded(recorded.clone()), ADDRESS)
            .await
            .unwrap();

        assert_eq!(profile.point(), (52.4736, 13.4205));
        assert_eq!(profile.borough.as_deref(), Some("YourDistrict"));
        assert_eq!(profile.postcode.as_deref(), Some("10000"));
        assert_eq!(profile.nina_ags, None, "outside Germany");
        assert_eq!(recorded.calls().len(), 1);
        assert_eq!(db.home_profile().await.unwrap().unwrap().address, ADDRESS);
    }

    #[tokio::test]
    async fn a_german_city_gets_its_nina_region() {
        let answer = fixture("nominatim/reference.json")
            .replace("\"country_code\":\"xx\"", "\"country_code\":\"de\"")
            .replace("Yourcity", "Berlin");
        let recorded = Arc::new(Recorded::new().route(SEARCH, 200, &answer));

        let profile = resolve(&Http::recorded(recorded.clone()), ADDRESS)
            .await
            .unwrap();

        // A city-state's key is built in: no request to NINA.
        assert_eq!(profile.nina_ags.as_deref(), Some("110000000000"));
        assert_eq!(recorded.calls().len(), 1);
    }

    fn address_only() -> HomeConfig {
        HomeConfig {
            address: Some(ADDRESS.to_owned()),
            point: None,
        }
    }

    /// The status of the supervised report `name`, once `done` holds (it is set after the
    /// profile is published).
    async fn status_when(
        name: &str,
        done: impl Fn(&mxbot_common::supervise::TaskStatus) -> bool,
    ) -> mxbot_common::supervise::TaskStatus {
        for _ in 0..500 {
            if let Some(status) = mxbot_common::supervise::snapshot()
                .into_iter()
                .find(|t| t.name == name)
                .filter(|t| done(t))
            {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("{name} never reached the expected state");
    }

    const FAST: (Duration, Duration) = (Duration::from_millis(5), Duration::from_millis(20));

    #[tokio::test]
    async fn the_loops_start_once_a_later_attempt_succeeds() {
        let (db, _dir) = test_db();
        let recorded = Arc::new(Recorded::new().route_failing_first(
            SEARCH,
            2,
            200,
            &fixture("nominatim/reference.json"),
        ));
        let alive = mxbot_common::supervise::report("radar profile under test");
        let (publish, mut published) = watch::channel(None);

        let task = tokio::spawn(keep_profile(
            db.clone(),
            Http::recorded(recorded.clone()),
            address_only(),
            alive,
            FAST,
            publish,
        ));
        let profile =
            tokio::time::timeout(Duration::from_secs(5), published.wait_for(Option::is_some))
                .await
                .expect("started")
                .unwrap()
                .clone()
                .unwrap();
        let status = status_when("radar profile under test", |t| t.ok_total > 0).await;
        task.abort();

        assert_eq!(profile.borough.as_deref(), Some("YourDistrict"));
        assert_eq!(recorded.calls().len(), 3);
        assert_eq!((status.failed_total, status.ok_total), (2, 1));
        assert!(!published.has_changed().unwrap_or(false), "started once");
    }

    #[tokio::test]
    async fn a_configured_point_starts_the_loops_without_the_geocoder() {
        let (db, _dir) = test_db();
        let recorded = Arc::new(Recorded::new());
        let alive = mxbot_common::supervise::report("radar profile, point under test");
        let (publish, published) = watch::channel(None);
        let home = HomeConfig {
            address: Some(ADDRESS.to_owned()),
            point: Some((52.4736, 13.4205)),
        };

        let task = tokio::spawn(keep_profile(
            db,
            Http::recorded(recorded.clone()),
            home,
            alive,
            FAST,
            publish,
        ));
        let status = status_when("radar profile, point under test", |t| t.idle_total > 0).await;
        task.abort();

        let profile = published.borrow().clone().expect("published at once");
        assert_eq!(profile.point(), (52.4736, 13.4205));
        assert_eq!(profile.borough, None, "details wait for the geocoder");
        assert_eq!(status.failed_total, 0, "idle, not failed");
        assert!(
            !recorded.calls().is_empty(),
            "the details are still asked for"
        );
    }

    #[tokio::test]
    async fn a_changed_address_keeps_the_stored_profile_until_it_is_resolved() {
        let (db, _dir) = test_db();
        let mut old = stored();
        old.address = "Oldstreet 9, 10000 Yourcity".to_owned();
        db.store_home_profile(&old).await.unwrap();
        let alive = mxbot_common::supervise::report("radar profile, moved under test");
        let (publish, published) = watch::channel(None);

        let task = tokio::spawn(keep_profile(
            db,
            Http::offline(),
            address_only(),
            alive,
            FAST,
            publish,
        ));
        let status = status_when("radar profile, moved under test", |t| t.failed_total > 0).await;
        task.abort();

        assert_eq!(published.borrow().clone(), Some(old));
        assert_eq!(
            status.last_err_reason.as_deref(),
            Some("address changed, not resolved yet")
        );
    }

    #[tokio::test]
    async fn a_failed_nina_lookup_leaves_the_region_unknown_and_is_tried_again() {
        let (db, _dir) = test_db();
        let answer = fixture("nominatim/reference.json")
            .replace("\"country_code\":\"xx\"", "\"country_code\":\"de\"");
        let recorded = Arc::new(
            Recorded::new()
                .route(SEARCH, 200, &answer)
                .route_failing_first(
                    "https://warnung.bund.de/api31/completion/search",
                    2,
                    200,
                    r#"[{"id":"099990000000","name":"Yourcity"}]"#,
                ),
        );
        let alive = mxbot_common::supervise::report("radar profile, NINA under test");
        let (publish, mut published) = watch::channel(None);

        let task = tokio::spawn(keep_profile(
            db.clone(),
            Http::recorded(recorded.clone()),
            address_only(),
            alive,
            FAST,
            publish,
        ));
        let first = published
            .wait_for(Option::is_some)
            .await
            .unwrap()
            .clone()
            .unwrap();
        assert_eq!(
            first.nina_ags, None,
            "the lookup failed: unknown, not stored as known"
        );
        let with_region = tokio::time::timeout(
            Duration::from_secs(5),
            published.wait_for(|p| p.as_ref().is_some_and(|p| p.nina_ags.is_some())),
        )
        .await
        .expect("looked up again")
        .unwrap()
        .clone()
        .unwrap();
        task.abort();

        assert_eq!(with_region.nina_ags.as_deref(), Some("099990000000"));
        let searches = recorded
            .calls()
            .iter()
            .filter(|c| c.contains("nominatim"))
            .count();
        assert_eq!(searches, 1, "only the region is asked again");
        assert_eq!(
            db.home_profile()
                .await
                .unwrap()
                .unwrap()
                .nina_ags
                .as_deref(),
            Some("099990000000")
        );
    }

    #[tokio::test]
    async fn the_nina_region_search_refuses_bad_answers() {
        for (status, body) in [(503, "[]"), (200, "{\"error\": 1}"), (200, "<html>")] {
            let http = Http::recorded(Arc::new(Recorded::new().route(
                "https://warnung.bund.de/",
                status,
                body,
            )));
            assert!(
                crate::alerts::nina_ags_for_city(&http, "Yourcity")
                    .await
                    .is_err(),
                "{status} {body}"
            );
        }
        let http = Http::recorded(Arc::new(Recorded::new().route(
            "https://warnung.bund.de/",
            200,
            "[]",
        )));
        assert_eq!(
            crate::alerts::nina_ags_for_city(&http, "Yourcity").await,
            Ok(None)
        );
    }

    #[tokio::test]
    async fn an_old_profile_is_resolved_again_at_start() {
        let (db, _dir) = test_db();
        db.store_home_profile(&stored()).await.unwrap(); // resolved_at = 1
        let recorded =
            Arc::new(Recorded::new().route(SEARCH, 200, &fixture("nominatim/reference.json")));
        let alive = mxbot_common::supervise::report("radar profile, refresh under test");
        let (publish, _published) = watch::channel(None);

        let task = tokio::spawn(keep_profile(
            db.clone(),
            Http::recorded(recorded.clone()),
            address_only(),
            alive,
            FAST,
            publish,
        ));
        status_when("radar profile, refresh under test", |t| t.ok_total > 0).await;
        task.abort();

        assert_eq!(recorded.calls().len(), 1);
        assert!(db.home_profile().await.unwrap().unwrap().resolved_at > 1);
    }
}
