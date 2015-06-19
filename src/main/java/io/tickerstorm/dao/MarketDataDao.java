package io.tickerstorm.dao;

import org.springframework.data.repository.CrudRepository;

public interface MarketDataDao extends CrudRepository<MarketDataDto, PrimaryKey> {

}
