//! The stored reference point (`crate::profile`).

use anyhow::Result;
use mxbot_common::sqlite::rusqlite::params;

use super::Db;
use crate::profile::HomeProfile;

impl Db {
    /// The stored reference point, if one was resolved.
    pub async fn home_profile(&self) -> Result<Option<HomeProfile>> {
        self.0.run(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT address, lat, lon, city, borough, postcode, country_code, nina_ags, resolved_at
                 FROM home_profile WHERE id = 1",
            )?;
            let mut rows = stmt.query([])?;
            let Some(row) = rows.next()? else {
                return Ok(None);
            };
            Ok(Some(HomeProfile {
                address: row.get(0)?,
                lat: row.get(1)?,
                lon: row.get(2)?,
                city: row.get(3)?,
                borough: row.get(4)?,
                postcode: row.get(5)?,
                country_code: row.get(6)?,
                nina_ags: row.get(7)?,
                resolved_at: row.get(8)?,
            }))
        })
        .await
    }

    /// Store `profile` as the reference point, replacing the one before.
    pub async fn store_home_profile(&self, profile: &HomeProfile) -> Result<()> {
        let p = profile.clone();
        self.0.run(move |conn| {
            conn.execute(
                "INSERT INTO home_profile
                     (id, address, lat, lon, city, borough, postcode, country_code, nina_ags, resolved_at)
                 VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(id) DO UPDATE SET
                     address = excluded.address, lat = excluded.lat, lon = excluded.lon,
                     city = excluded.city, borough = excluded.borough,
                     postcode = excluded.postcode, country_code = excluded.country_code,
                     nina_ags = excluded.nina_ags, resolved_at = excluded.resolved_at",
                params![
                    p.address,
                    p.lat,
                    p.lon,
                    p.city,
                    p.borough,
                    p.postcode,
                    p.country_code,
                    p.nina_ags,
                    p.resolved_at
                ],
            )?;
            Ok(())
        })
        .await
    }
}
