import sys, pathlib
_d = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(_d if (_d / 'monitor.py').exists() else _d.parent))
import os; os.chdir(pathlib.Path(__file__).resolve().parent.parent if (pathlib.Path(__file__).resolve().parent.parent / 'monitor.py').exists() else pathlib.Path(__file__).resolve().parent)
import importlib.util
spec=importlib.util.spec_from_file_location('m','monitor.py'); m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
KEEP=["高盛将铜目标价下调至9000美元","机构上调黄金目标价至4000美元","高盛上调铝目标价",
      "花旗银行下调锂市场目标价","美联储宣布降息","美联储宣布加息","美联储连续第二次降息",
      "美联储降息预期升温美元走弱","美联储维持利率不变","美联储加息50BP","央行降准1个百分点",
      "玻璃现货价格","苹果主产区遭遇霜冻减产","玻璃厂家库存回升","苹果库存下降","浮法玻璃生产线冷修",
      "欧盟决定对华金属硅进口实施登记",
      "玻璃现货价格", "苹果主产区遭遇霜冻减产","雪佛龙项目增加85000桶/日的产能","伊朗南帕尔斯炼气厂产能修复"]
DROP=["美联储官员：下次加息仍需观察", "美联储：首次加息或提前", "美联储再次加息的可能性",
      "苹果产地物流园签约仪式举行", "玻璃厂股权收购完成","老铺黄金目标价上调至1200港元","野村将老铺黄金目标价砍半至450港元",
      "美联储洛根：最终目标是让政策适度收紧","美联储12月加息的概率约为79%",
      "苹果CEO库克：关税退款影响毛利率",
      "苹果2027年将推出新款折叠屏手机","玻璃杯套餐销量增长","玻璃纤维行业景气度上行"]
bad=0
for t in KEEP:
    r=m.is_relevant(t,m.DEFAULT_KEYWORDS,True)
    if not r: bad+=1; print(f"BAD KEEP {m.filter_reason(t,m.DEFAULT_KEYWORDS,True)}  {t}")
for t in DROP:
    r=m.is_relevant(t,m.DEFAULT_KEYWORDS,True)
    if r: bad+=1; print(f"BAD DROP {t}")
print("probe4:", "PASS" if bad==0 else f"{bad} BAD")
