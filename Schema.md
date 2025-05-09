# dsci551_project

Database Scheme:

**MySQL**

Table: Listings

id (PK), listing_url, name, property_type, room_type, accommodates, bathrooms, bedrooms, beds, bed_type, description, neighborhood_overview, city, state, zipcode, latitude, longitude, is_location_exact
 
Table: Hosts

host_id (PK), host_url, host_name, host_since, host_location, host_about, host_is_superhost, host_has_profile_pic, host_identity_verified, host_listings_count
 
Table: Reviews

id (FK referencing listing), number_of_reviews, number_of_reviews_ltm, first_review, last_review, review_scores_rating, review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value, reviews_per_month


**MongoDB**

Colleciton: listings_meta 

id (key), scrape_id, last_scraped, source, host_id, host_response_time, host_response_rate, host_acceptance_rate, instant_bookable, license, neighbourhood_cleansed, neighbourhood_group_cleansed, market, smart_location, country_code, country

Collection: amenities

listing_id (key reference id), amenities (array of strings)

Collection: media

listing_id (key reference id), picture_url, host_thumbnail_url, host_picture_url, medium_url, xl_picture_url


**Firebase**

Table: /listings/{id}/pricing

id (document key), price, weekly_price, monthly_price, security_deposit, cleaning_fee, guests_included, extra_people

Table: /listings/{id}/availability

id (document key), calendar_updated, has_availability, availability_30, availability_60, availability_90, availability_365, calendar_last_scraped

