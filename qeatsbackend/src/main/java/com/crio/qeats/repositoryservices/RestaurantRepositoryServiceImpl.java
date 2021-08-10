/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.repositoryservices;

import ch.hsr.geohash.GeoHash;
import com.crio.qeats.configs.RedisConfiguration;
import com.crio.qeats.dto.Restaurant;
import com.crio.qeats.globals.GlobalConstants;
import com.crio.qeats.models.ItemEntity;
import com.crio.qeats.models.MenuEntity;
import com.crio.qeats.models.RestaurantEntity;
import com.crio.qeats.repositories.ItemRepository;
import com.crio.qeats.repositories.MenuRepository;
import com.crio.qeats.repositories.RestaurantRepository;
import com.crio.qeats.utils.GeoLocation;
import com.crio.qeats.utils.GeoUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Provider;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;


@Service
public class RestaurantRepositoryServiceImpl implements RestaurantRepositoryService {

  @Autowired
  private RedisConfiguration redisConfiguration;

  @Autowired
  private MongoTemplate mongoTemplate;

  @Autowired
  private Provider<ModelMapper> modelMapperProvider;

  @Autowired
  private RestaurantRepository restaurantRepository;

  @Autowired
  private ItemRepository itemRepository;

  @Autowired
  private MenuRepository menuRepository;

  private boolean isOpenNow(LocalTime time, RestaurantEntity res) {
    LocalTime openingTime = LocalTime.parse(res.getOpensAt());
    LocalTime closingTime = LocalTime.parse(res.getClosesAt());

    return time.isAfter(openingTime) && time.isBefore(closingTime);
  }

  // TODO: CRIO_TASK_MODULE_NOSQL
  // Objectives:
  // 1. Implement findAllRestaurantsCloseby.
  // 2. Remember to keep the precision of GeoHash in mind while using it as a key.
  // Check RestaurantRepositoryService.java file for the interface contract.
  public List<Restaurant> findAllRestaurantsCloseBy(Double latitude,
      Double longitude, LocalTime currentTime, Double servingRadiusInKms) {

    List<Restaurant> restaurants = new ArrayList<Restaurant>();
    GeoHash geoHash = GeoHash.withCharacterPrecision(latitude, longitude, 7);
    Jedis jedis = redisConfiguration.getJedisPool().getResource();
    ObjectMapper objectMapper = new ObjectMapper();
    String restaurantsString;
    
    // If restaurants exist in cache
    if (jedis.exists(geoHash.toBase32())) {
      restaurantsString = jedis.get(geoHash.toBase32());
      try {
        restaurants = objectMapper.readValue(restaurantsString, 
                      new TypeReference<List<Restaurant>>(){});
        return restaurants;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    
    // If restaurants don't exist in cache
    ModelMapper modelMapper = modelMapperProvider.get();
    List<RestaurantEntity> restaurantList = restaurantRepository.findAll();
    for (RestaurantEntity restaurantEntity: restaurantList) {
      if (isRestaurantCloseByAndOpen(restaurantEntity, currentTime, 
          latitude, longitude, servingRadiusInKms)) {
        Restaurant restaurant = modelMapper.map(restaurantEntity, Restaurant.class);
        restaurants.add(restaurant);
      }
    }
    try {
      restaurantsString = objectMapper.writeValueAsString(restaurants);
      jedis.set(geoHash.toBase32(),restaurantsString);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return restaurants;
  }

  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants whose names have an exact or partial match with the search query.
  @Override
  public List<Restaurant> findRestaurantsByName(Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {
    ModelMapper modelMapper = modelMapperProvider.get();
    Set<String> restaurantIdSet = new HashSet<>();
    List<Restaurant> restaurants = new ArrayList<>();

    Optional<List<RestaurantEntity>> optionalRestaurantsByName = 
        restaurantRepository.findRestaurantsByName(searchString);
    if (optionalRestaurantsByName.isPresent()) {
      List<RestaurantEntity> restaurantEntities = optionalRestaurantsByName.get();
      for (RestaurantEntity restaurantEntity: restaurantEntities) {
        if (isRestaurantCloseByAndOpen(restaurantEntity, currentTime, 
            latitude, longitude, servingRadiusInKms)
            && !restaurantIdSet.contains(restaurantEntity.getId())) {
          restaurants.add(modelMapper.map(restaurantEntity, Restaurant.class));
          restaurantIdSet.add(restaurantEntity.getRestaurantId());
        }
      }
    }
    
    Optional<List<RestaurantEntity>> optionalRestaurantsByExactName = 
        restaurantRepository.findRestaurantsByNameExact(searchString);
    if (optionalRestaurantsByExactName.isPresent()) {
      List<RestaurantEntity> restaurantEntities = optionalRestaurantsByExactName.get();
      for (RestaurantEntity restaurantEntity: restaurantEntities) {
        if (isRestaurantCloseByAndOpen(restaurantEntity, currentTime, 
            latitude, longitude, servingRadiusInKms)
            && !restaurantIdSet.contains(restaurantEntity.getId())) {
          restaurants.add(modelMapper.map(restaurantEntity, Restaurant.class));
          restaurantIdSet.add(restaurantEntity.getRestaurantId());
        }
      }
    }
    return restaurants;
  }


  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants whose attributes (cuisines) intersect with the search query.
  @Override
  public List<Restaurant> findRestaurantsByAttributes(
      Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {
    
    Query query = new Query();
    for (String str: searchString.split(" ")) {
      Pattern pattern = Pattern.compile(str, Pattern.CASE_INSENSITIVE);
      query.addCriteria(Criteria.where("attributes").regex(pattern));
    }
    
    List<Restaurant> restaurants = new ArrayList<>();
    ModelMapper modelMapper = modelMapperProvider.get();
    List<RestaurantEntity> restaurantEntityList = mongoTemplate.find(query, RestaurantEntity.class);
    for (RestaurantEntity restaurantEntity: restaurantEntityList) {
      if (isRestaurantCloseByAndOpen(restaurantEntity, currentTime, 
          latitude, longitude, servingRadiusInKms)) {
        restaurants.add(modelMapper.map(restaurantEntity, Restaurant.class));
      }
    }
    return restaurants;
  }



  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants which serve food items whose names form a complete or partial match
  // with the search query.

  @Override
  public List<Restaurant> findRestaurantsByItemName(
      Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {
    
    String regex = String.join("|", Arrays.asList(searchString.split(" ")));
    Optional<List<ItemEntity>> optionalExactItems
        = itemRepository.findItemsByNameExact(searchString);
    Optional<List<ItemEntity>> optionalInexactItems
        = itemRepository.findItemsByNameInexact(regex);

    List<ItemEntity> itemEntityList = optionalExactItems.orElseGet(ArrayList::new);
    List<ItemEntity> inexactItemEntityList = optionalInexactItems.orElseGet(ArrayList::new);
    itemEntityList.addAll(inexactItemEntityList);

    return getRestaurantListServingItems(latitude, longitude, currentTime, servingRadiusInKms,
        itemEntityList);
    
  }

  private List<Restaurant> getRestaurantListServingItems(Double latitude, Double longitude,
      LocalTime currentTime, Double servingRadiusInKms, List<ItemEntity> itemEntityList) {

    List<String> itemIdList = itemEntityList
        .stream()
        .map(ItemEntity::getItemId)
        .collect(Collectors.toList());

    Optional<List<MenuEntity>> optionalMenuEntityList
        = menuRepository.findMenusByItemsItemIdIn(itemIdList);
    Optional<List<RestaurantEntity>> optionalRestaurantEntityList = Optional.empty();

    if (optionalMenuEntityList.isPresent()) {
      List<MenuEntity> menuEntityList = optionalMenuEntityList.get();
      List<String> restaurantIdList = menuEntityList
          .stream()
          .map(MenuEntity::getRestaurantId)
          .collect(Collectors.toList());
      optionalRestaurantEntityList = restaurantRepository
          .findRestaurantsByRestaurantIdIn(restaurantIdList);
    }

    List<Restaurant> restaurantList = new ArrayList<>();
    ModelMapper modelMapper = modelMapperProvider.get();
    if (optionalRestaurantEntityList.isPresent()) {
      List<RestaurantEntity> restaurantEntityList = optionalRestaurantEntityList.get();

      List<RestaurantEntity> restaurantEntitiesFiltered = new ArrayList<>();

      for (RestaurantEntity restaurantEntity : restaurantEntityList) {
        if (isRestaurantCloseByAndOpen(restaurantEntity, currentTime, latitude, longitude,
            servingRadiusInKms)) {
          restaurantEntitiesFiltered.add(restaurantEntity);
        }
      }

      restaurantList = restaurantEntitiesFiltered
          .stream()
          .map(restaurantEntity -> modelMapper.map(restaurantEntity, Restaurant.class))
          .collect(Collectors.toList());
    }

    return restaurantList;
  }

  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants which serve food items whose attributes intersect with the search query.
  @Override
  public List<Restaurant> findRestaurantsByItemAttributes(Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {
    
    List<Pattern> patterns = Arrays
        .stream(searchString.split(" "))
        .map(attr -> Pattern.compile(attr, Pattern.CASE_INSENSITIVE))
        .collect(Collectors.toList());
    Query query = new Query();
    for (Pattern pattern : patterns) {
      query.addCriteria(
          Criteria.where("attributes").regex(pattern)
      );
    }

    List<ItemEntity> itemEntityList = mongoTemplate.find(query, ItemEntity.class);
    return getRestaurantListServingItems(latitude, longitude, currentTime, servingRadiusInKms,
        itemEntityList);
  }

  /**
   * Utility method to check if a restaurant is within the serving radius at a given time.
   * @return boolean True if restaurant falls within serving radius and is open, false otherwise
   */
  private boolean isRestaurantCloseByAndOpen(RestaurantEntity restaurantEntity,
      LocalTime currentTime, Double latitude, Double longitude, Double servingRadiusInKms) {
    if (isOpenNow(currentTime, restaurantEntity)) {
      return GeoUtils.findDistanceInKm(latitude, longitude,
          restaurantEntity.getLatitude(), restaurantEntity.getLongitude())
          < servingRadiusInKms;
    }

    return false;
  }

  @Override
  public Future<List<Restaurant>> findRestaurantsByNameAsync(Double latitude, 
      Double longitude, String searchString, LocalTime currentTime, Double servingRadiusInKms) {
    List<Restaurant> restaurants = findRestaurantsByName(latitude, longitude, 
        searchString, currentTime, servingRadiusInKms);
    return new AsyncResult<>(restaurants);
  }

  @Override
  public Future<List<Restaurant>> findRestaurantsByAttributesAsync(Double latitude, 
      Double longitude, String searchString, LocalTime currentTime, Double servingRadiusInKms) {
    List<Restaurant> restaurants = findRestaurantsByAttributes(latitude, longitude, 
        searchString, currentTime, servingRadiusInKms);
    return new AsyncResult<>(restaurants);
  }

  @Override
  public Future<List<Restaurant>> findRestaurantsByItemNameAsync(Double latitude, 
      Double longitude, String searchString, LocalTime currentTime, Double servingRadiusInKms) {
    List<Restaurant> restaurants = findRestaurantsByItemName(latitude, longitude, 
        searchString, currentTime, servingRadiusInKms);
    return new AsyncResult<>(restaurants);
  }

  @Override
  public Future<List<Restaurant>> findRestaurantsByItemAttributesAsync(Double latitude, 
      Double longitude, String searchString, LocalTime currentTime, Double servingRadiusInKms) {
    List<Restaurant> restaurants = findRestaurantsByItemAttributes(latitude, longitude, 
        searchString, currentTime, servingRadiusInKms);
    return new AsyncResult<>(restaurants);
  }
}

