use axum::Json;

struct SearchResponse {
    lat: f32,
    lng: f32
}

trait RouteTrait {
    fn search(query: String) -> Json<SearchResponse>;
    fn poi(lat: f32, lng: f32) -> Json<SearchResponse>;
}