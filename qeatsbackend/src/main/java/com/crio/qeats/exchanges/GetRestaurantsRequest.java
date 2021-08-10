/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.exchanges;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@Data
@NoArgsConstructor
@RequiredArgsConstructor
@AllArgsConstructor
public class GetRestaurantsRequest {
  @NonNull
  @Min(value = -90)
  @Max(value = 90)
  private Double latitude;
  @NonNull
  @Min(value = -180)
  @Max(value = 180)
  private Double longitude;
  private String searchFor;
}

