import functools

__all__=[
    "params_chack",
     "return_check",
]

#check parameters of a function usage: @params_chack(int, str, (int, str), key1=list, key2=(list, tuple))
def params_chack(*types,**kwtypes):
    def _decoration(func):
        @functools.wraps(func)
        def _inner(*args,**kwargs):
            result=[isinstance(_param,_type)for _param,_type in zip(args,types)]
            assert  all(result), "params_chack: invalid parameters in function " + func.__name__
            result2=[isinstance(kwtypes[_param],kwargs[_param])for _param in kwargs if _param in kwtypes]
            assert all(result), "params_chack: invalid parameters in function " + func.__name__
            return func(*args, **kwargs)
        return _inner
    return _decoration

def return_check(*types):

    return_length=len(types)
    def _decoration(func):
        @functools.wraps(func)
        def _inner(*args,**kwargs):
            return_tuple= func(*args,**kwargs)
            if return_length>1:
                result=[isinstance(_return,_type)for _return,_type in zip(return_tuple,types)]
                assert all(result), "return_check: invalid return in function " + func.__name__
            elif return_length==1:
                assert isinstance(return_tuple,type[0]), "return_check: invalid return in function " + func.__name__
            else:
                pass
            return return_tuple
        return _inner
    return _decoration




