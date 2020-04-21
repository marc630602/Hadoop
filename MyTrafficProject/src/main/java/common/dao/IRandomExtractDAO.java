package common.dao;


import common.domain.RandomExtractCar;
import common.domain.RandomExtractMonitorDetail;

import java.util.List;

/**
 * 随机抽取car信息管理DAO类
 * @author root
 *
 */
public interface IRandomExtractDAO {
	void insertBatchRandomExtractCar(List<RandomExtractCar> carRandomExtracts);
	
	void insertBatchRandomExtractDetails(List<RandomExtractMonitorDetail> r);
	
}
