
/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.services;

import com.crio.qeats.dto.Restaurant;
import com.crio.qeats.exchanges.GetRestaurantsRequest;
import com.crio.qeats.exchanges.GetRestaurantsResponse;
import com.crio.qeats.repositoryservices.RestaurantRepositoryService;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Log4j2
public class RestaurantServiceImpl implements RestaurantService {

  private final Double peakHoursServingRadiusInKms = 3.0;
  private final Double normalHoursServingRadiusInKms = 5.0;
  @Autowired
  private RestaurantRepositoryService restaurantRepositoryService;


  // TODO: CRIO_TASK_MODULE_RESTAURANTSAPI - Implement findAllRestaurantsCloseby.
  // Check RestaurantService.java file for the interface contract.
  @Override
  public GetRestaurantsResponse findAllRestaurantsCloseBy(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {
    List<Restaurant> restaurants;
    if (isPeakHour(currentTime)) {
      restaurants = restaurantRepositoryService.findAllRestaurantsCloseBy(
          getRestaurantsRequest.getLatitude(), getRestaurantsRequest.getLongitude(),
          currentTime, peakHoursServingRadiusInKms);
    } else {
      restaurants = restaurantRepositoryService.findAllRestaurantsCloseBy(
        getRestaurantsRequest.getLatitude(), getRestaurantsRequest.getLongitude(),
        currentTime, normalHoursServingRadiusInKms);
    }
    GetRestaurantsResponse response = new GetRestaurantsResponse(restaurants);
    //log.info(response);
    return response;
  }

  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Implement findRestaurantsBySearchQuery. The request object has the search string.
  // We have to combine results from multiple sources:
  // 1. Restaurants by name (exact and inexact)
  // 2. Restaurants by cuisines (also called attributes)
  // 3. Restaurants by food items it serves
  // 4. Restaurants by food item attributes (spicy, sweet, etc)
  // Remember, a restaurant must be present only once in the resulting list.
  // Check RestaurantService.java file for the interface contract.
  @Override
  public GetRestaurantsResponse findRestaurantsBySearchQuery(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {
    Double servingRadiusInKms = isPeakHour(currentTime) 
        ? peakHoursServingRadiusInKms : normalHoursServingRadiusInKms;
    
    String searchFor = getRestaurantsRequest.getSearchFor();
    List<Restaurant> restaurants = new ArrayList<>();
    List<List<Restaurant>> listOfRestaurantLists = new ArrayList<>();
    // If there is a search query
    if (searchFor != null && !searchFor.isEmpty()) {
      listOfRestaurantLists.add(restaurantRepositoryService
          .findRestaurantsByName(getRestaurantsRequest.getLatitude(), 
          getRestaurantsRequest.getLongitude(), searchFor, currentTime, servingRadiusInKms));
      listOfRestaurantLists.add(restaurantRepositoryService
          .findRestaurantsByAttributes(getRestaurantsRequest.getLatitude(), 
          getRestaurantsRequest.getLongitude(), searchFor, currentTime, servingRadiusInKms));
      listOfRestaurantLists.add(restaurantRepositoryService
          .findRestaurantsByItemName(getRestaurantsRequest.getLatitude(), 
          getRestaurantsRequest.getLongitude(), searchFor, currentTime, servingRadiusInKms));
      listOfRestaurantLists.add(restaurantRepositoryService
          .findRestaurantsByItemAttributes(getRestaurantsRequest.getLatitude(), 
          getRestaurantsRequest.getLongitude(), searchFor, currentTime, servingRadiusInKms));
      
      Set<String> restaurantIdSet = new HashSet<>();
      for (List<Restaurant> listOfRestaurant: listOfRestaurantLists) {
        for (Restaurant restaurant : listOfRestaurant) {
          if (!restaurantIdSet.contains(restaurant.getRestaurantId())) {
            restaurants.add(restaurant);
            restaurantIdSet.add(restaurant.getRestaurantId());
          }
        }
      }
    }
    GetRestaurantsResponse response = new GetRestaurantsResponse(restaurants);
    log.info(response);
    return response;
  }

  public boolean isTimeInRange(LocalTime time, LocalTime startTime, LocalTime endTime) {
    return time.isAfter(startTime) && time.isBefore(endTime);
  }

  public boolean isPeakHour(LocalTime time) {
    return isTimeInRange(time, LocalTime.of(7, 59, 59), 
    LocalTime.of(10, 00, 01)) 
      || isTimeInRange(time, LocalTime.of(12, 59, 59), 
    LocalTime.of(14, 00, 01))
      || isTimeInRange(time, LocalTime.of(18, 59, 59), 
    LocalTime.of(21, 00, 01));
  }

  // TODO: CRIO_TASK_MODULE_MULTITHREADING
  // Implement multi-threaded version of RestaurantSearch.
  // Implement variant of findRestaurantsBySearchQuery which is at least 1.5x time faster than
  // findRestaurantsBySearchQuery.
  @Override
  public GetRestaurantsResponse findRestaurantsBySearchQueryMt(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {

    Double servingRadiusInKms = isPeakHour(currentTime) 
        ? peakHoursServingRadiusInKms : normalHoursServingRadiusInKms;
    
    String searchFor = getRestaurantsRequest.getSearchFor();
    List<Restaurant> restaurants;

    if (!searchFor.isEmpty()) {
      long startTime = System.currentTimeMillis();

      Future<List<Restaurant>> futureRestaurantsByName = restaurantRepositoryService
          .findRestaurantsByNameAsync(getRestaurantsRequest.getLatitude(), 
          getRestaurantsRequest.getLongitude(), searchFor, currentTime, servingRadiusInKms);
      Future<List<Restaurant>> futureRestaurantsByAttributes = restaurantRepositoryService
          .findRestaurantsByAttributesAsync(getRestaurantsRequest.getLatitude(), 
          getRestaurantsRequest.getLongitude(), searchFor, currentTime, servingRadiusInKms);

      List<Restaurant> restaurantsByName;
      List<Restaurant> restaurantsByAttributes;

      try {
        while (true) {
          if (futureRestaurantsByName.isDone() && futureRestaurantsByAttributes.isDone()) {
            restaurantsByName = futureRestaurantsByName.get();
            restaurantsByAttributes = futureRestaurantsByAttributes.get();
            log.info("Time taken in millis: " + (System.currentTimeMillis() - startTime));
            break;
          }
        }
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
        return new GetRestaurantsResponse(new ArrayList<>());
      }

      Map<String,Restaurant> restaurantMap = new HashMap<>();
      for (Restaurant r: restaurantsByName) {
        restaurantMap.put(r.getRestaurantId(), r);
      }
      for (Restaurant r: restaurantsByAttributes) {
        restaurantMap.put(r.getRestaurantId(), r);
      }
      restaurants = new ArrayList<>(restaurantMap.values());
    } else {
      restaurants = new ArrayList<>();
    }

    return new GetRestaurantsResponse(restaurants);
  }
}

