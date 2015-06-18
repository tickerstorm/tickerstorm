package io.tickerstorm.dao;

import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.NoRepositoryBean;

@NoRepositoryBean
public interface MarketDataDao extends CrudRepository<MarketDataDto, PrimaryKey> {

}
