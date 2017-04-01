#_*_ coding: utf-8 _*_
"""

"""
import random
from .util_config import CONFIG_USERAGENT_PC, CONFIG_USERAGENT_PHONE, CONFIG_USERAGENT_ALL

__all__=["make_random_useragent",]

def make_random_useragent(ua_type="pc"):
    #make a random user_agent based on ua_type(pc,phone,all)
    ua_type=ua_type.lower()
    assert ua_type in("pc","phone","all"), "make_random_useragent: parameter ua_type[%s] is invalid" % ua_type
    return random.choice(CONFIG_USERAGENT_PC if ua_type == "pc" else (CONFIG_USERAGENT_PHONE if ua_type == "phone" else CONFIG_USERAGENT_ALL))