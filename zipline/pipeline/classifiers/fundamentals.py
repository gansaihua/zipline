import numpy as np
from zipline.utils.numpy_utils import int64_dtype
from zipline.pipeline.data import Fundamentals
from .classifier import CustomClassifier


class Sector(CustomClassifier):
    ENERGY = 882001
    MATERIALS = 882002
    INDUSTRIALS = 882003
    CONSUMER_DISCRETIONARY = 882004
    CONSUMER_STAPLES = 882005
    HEALTH_CARE = 882006
    FINANCIALS = 882007
    INFORMATION_TECHNOLOGY = 882008
    COMMUNICATION_SERVICES = 882009
    UTILITIES = 882010
    REAL_ESTATE = 882011

    SECTOR_NAMES = {
        882001: 'ENERGY',
        882002: 'MATERIALS',
        882003: 'INDUSTRIALS',
        882004: 'CONSUMER_DISCRETIONARY',
        882005: 'CONSUMER_STAPLES',
        882006: 'HEALTH_CARE',
        882007: 'FINANCIALS',
        882008: 'INFORMATION_TECHNOLOGY',
        882009: 'COMMUNICATION_SERVICES',
        882010: 'UTILITIES',
        882011: 'REAL_ESTATE',
        -1: 'Unknown',
    }

    inputs = [
        Fundamentals.memb_energy,
        Fundamentals.memb_materials,
        Fundamentals.memb_industrials,
        Fundamentals.memb_consumer_discretionary,
        Fundamentals.memb_consumer_staples,
        Fundamentals.memb_health_care,
        Fundamentals.memb_financials,
        Fundamentals.memb_information_technology,
        Fundamentals.memb_communication_services,
        Fundamentals.memb_utilities,
        Fundamentals.memb_real_estate,
    ]
    window_length = 1
    missing_value = -1
    dtype = int64_dtype

    def compute(self, today, assets, out, *sectors):
        mul = [882001 + i for i in range(12)]
        out[:] = np.nansum(
            np.array([sector[0] * m for sector, m in zip(sectors, mul)]),
            axis=0, dtype=self.dtype,
        )
        out[np.isnan(out)] = self.missing_value


class Industry(CustomClassifier):
    AGRICULTURE = 801010
    MINING = 801020
    CHEMICALS = 801030
    STEEL = 801040
    METALS = 801050
    ELECTRONICS = 801080
    APPLIANCES = 801110
    FOOD = 801120
    TEXTILES = 801130
    LIGHT_MANUFACTURING = 801140
    PHARMACEUTICALS = 801150
    UTILITIES = 801160
    TRANSPORTATION = 801170
    REAL_ESTATE = 801180
    COMMERCE = 801200
    SERVICES = 801210
    CONGLOMERATE = 801230
    BUILDING_MATERIALS = 801710
    BUILDING_DECORATIONS = 801720
    ELECTRICALS = 801730
    DEFENSE_MILITARY = 801740
    IT = 801750
    MEDIA = 801760
    COMMUNICATION_SERVICES = 801770
    BANKS = 801780
    NONBANK_FINANCIALS = 801790
    AUTO = 801880
    MACHINERY = 801890

    INDUSTRY_NAMES = {
        801010: 'AGRICULTURE',  # 农林牧渔
        801020: 'MINING',  # 采掘
        801030: 'CHEMICALS',  # 化工
        801040: 'STEEL',  # 钢铁
        801050: 'METALS',  # 有色金属
        801080: 'ELECTRONICS',  # 电子
        801110: 'APPLIANCES',  # 家用电器
        801120: 'FOOD',  # 食品饮料
        801130: 'TEXTILES',  # 纺织服装
        801140: 'LIGHT_MANUFACTURING',  # 轻工制造
        801150: 'PHARMACEUTICALS',  # 医药生物
        801160: 'UTILITIES',  # 公用事业
        801170: 'TRANSPORTATION',  # 交通运输
        801180: 'REAL_ESTATE',  # 房地产
        801200: 'COMMERCE',  # 商业贸易
        801210: 'SERVICES',  # 休闲服务
        801230: 'CONGLOMERATE',  # 综合
        801710: 'BUILDING_MATERIALS',  # 建筑材料
        801720: 'BUILDING_DECORATIONS',  # 建筑装饰
        801730: 'ELECTRICALS',  # 电气设备
        801740: 'DEFENSE_MILITARY',  # 国防军工
        801750: 'IT',  # 计算机
        801760: 'MEDIA',  # 传媒
        801770: 'COMMUNICATION_SERVICES',  # 通信
        801780: 'BANKS',  # 银行
        801790: 'NONBANK_FINANCIALS',  # 非银金融
        801880: 'AUTO',  # 汽车
        801890: 'MACHINERY',  # 机械设备
        -1: 'Unknown',
    }
    inputs = [
        Fundamentals.memb_sw_agriculture,
        Fundamentals.memb_sw_mining,
        Fundamentals.memb_sw_chemicals,
        Fundamentals.memb_sw_steel,
        Fundamentals.memb_sw_metals,
        Fundamentals.memb_sw_electronics,
        Fundamentals.memb_sw_appliances,
        Fundamentals.memb_sw_food,
        Fundamentals.memb_sw_textiles,
        Fundamentals.memb_sw_light_manufacturing,
        Fundamentals.memb_sw_pharmaceuticals,
        Fundamentals.memb_sw_utilities,
        Fundamentals.memb_sw_transportation,
        Fundamentals.memb_sw_real_estate,
        Fundamentals.memb_sw_commerce,
        Fundamentals.memb_sw_services,
        Fundamentals.memb_sw_conglomerate,
        Fundamentals.memb_sw_building_materials,
        Fundamentals.memb_sw_building_decorations,
        Fundamentals.memb_sw_electricals,
        Fundamentals.memb_sw_defense_military,
        Fundamentals.memb_sw_it,
        Fundamentals.memb_sw_media,
        Fundamentals.memb_sw_communication_services,
        Fundamentals.memb_sw_banks,
        Fundamentals.memb_sw_nonbank_financials,
        Fundamentals.memb_sw_auto,
        Fundamentals.memb_sw_machinery,
    ]
    window_length = 1
    missing_value = -1
    dtype = int64_dtype

    def compute(self, today, assets, out, *industries):
        mul = [
            801010, 801020, 801030, 801040, 801050, 801080, 801110, 801120, 801130, 801140,
            801150, 801160, 801170, 801180, 801200, 801210, 801230, 801710, 801720, 801730,
            801740, 801750, 801760, 801770, 801780, 801790, 801880, 801890,
        ]
        out[:] = np.nansum(
            np.array([industry[0] * m for industry, m in zip(industries, mul)]),
            axis = 0, dtype = self.dtype,
        )
        out[np.isnan(out)] = self.missing_value
