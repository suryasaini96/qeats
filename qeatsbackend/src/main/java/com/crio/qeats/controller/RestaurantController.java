/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.controller;

import com.crio.qeats.dto.Restaurant;
import com.crio.qeats.exchanges.GetRestaurantsRequest;
import com.crio.qeats.exchanges.GetRestaurantsResponse;
import com.crio.qeats.services.RestaurantService;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import javax.validation.Valid;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Log4j2
@RequestMapping(RestaurantController.RESTAURANT_API_ENDPOINT)
public class RestaurantController {

  public static final String RESTAURANT_API_ENDPOINT = "/qeats/v1";
  public static final String RESTAURANTS_API = "/restaurants";
  public static final String MENU_API = "/menu";
  public static final String CART_API = "/cart";
  public static final String CART_ITEM_API = "/cart/item";
  public static final String CART_CLEAR_API = "/cart/clear";
  public static final String POST_ORDER_API = "/order";
  public static final String GET_ORDERS_API = "/orders";

  @Autowired
  private RestaurantService restaurantService;

  @GetMapping(RESTAURANTS_API)
  public ResponseEntity<GetRestaurantsResponse> getRestaurants(
       GetRestaurantsRequest getRestaurantsRequest) {

    log.info("getRestaurants called with {}", getRestaurantsRequest);
    GetRestaurantsResponse getRestaurantsResponse;

    if (getRestaurantsRequest.getLatitude() != null && getRestaurantsRequest.getLongitude() != null
        && getRestaurantsRequest.getLatitude() >= -90 && getRestaurantsRequest.getLatitude() <= 90
        && getRestaurantsRequest.getLongitude() >= -180 
        && getRestaurantsRequest.getLongitude() <= 180) {

      List<Restaurant> restaurants = new ArrayList<>();
      // If searching by searchFor string
      if (getRestaurantsRequest.getSearchFor() != null 
          && !getRestaurantsRequest.getSearchFor().isEmpty()) {
        getRestaurantsResponse = restaurantService
          .findRestaurantsBySearchQuery(getRestaurantsRequest, LocalTime.now());
        if (getRestaurantsResponse == null) {
          return ResponseEntity.ok().body(null);
        }
        restaurants = getRestaurantsResponse.getRestaurants();
      } else { // Get nearby restaurants
        getRestaurantsResponse = restaurantService
          .findAllRestaurantsCloseBy(getRestaurantsRequest, LocalTime.now());
        restaurants = getRestaurantsResponse.getRestaurants();
      }

      for (Restaurant r : restaurants) {
        String str = r.getName().replaceAll("[Â©éí]", "e");
        r.setName(str);
      }
      log.info("getRestaurants returned {}", getRestaurantsResponse);
      getRestaurantsResponse.setRestaurants(restaurants);
      return ResponseEntity.ok().body(getRestaurantsResponse);
    } else {
      return ResponseEntity.badRequest().body(null);
    }
  }
}

