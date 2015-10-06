package io.tickerstorm.data.dao;

import org.springframework.data.repository.CrudRepository;

public interface MarketDataDao extends CrudRepository<MarketDataDto, PrimaryKey> {

}
