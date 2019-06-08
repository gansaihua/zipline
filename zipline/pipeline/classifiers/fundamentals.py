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
    }

    inputs = [Fundamentals.wind_sector]
    window_length = 1
    dtype = int64_dtype
    missing_value = -1

    def compute(self, today, assets, out, cats):
        flag = ~np.isnan(cats[-1])
        out[flag] = cats[-1, flag]


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
    inputs = [Fundamentals.sw_industry]
    window_length = 1
    dtype = int64_dtype
    missing_value = -1

    def compute(self, today, assets, out, cats):
        flag = ~np.isnan(cats[-1])
        out[flag] = cats[-1, flag]
