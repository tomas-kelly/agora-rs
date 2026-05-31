//! Standalone example: food preferences data model.
//! Demonstrates serde-derived structs without any AWS SDK dependencies.

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct FoodItem {
    name: String,
    category: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct UserPreference {
    user_id: String,
    preference_id: String,
    food_item: FoodItem,
    rating: u8,
}

#[derive(Debug, Serialize, Deserialize)]
struct PreferenceList {
    items: Vec<UserPreference>,
}

fn main() {
    let list = PreferenceList {
        items: vec![UserPreference {
            user_id: "u_abc123".into(),
            preference_id: "pref_001".into(),
            food_item: FoodItem {
                name: "sushi".into(),
                category: "japanese".into(),
            },
            rating: 5,
        }],
    };
    println!("{}", serde_json::to_string_pretty(&list).unwrap());
}
