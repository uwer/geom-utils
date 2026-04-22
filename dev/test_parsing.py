

def test():
    from pygeom import convertDistanceIfUnit
    
    print(convertDistanceIfUnit("1000m"))
    print(convertDistanceIfUnit("1000mi"))
    print(convertDistanceIfUnit("1000mm"))
    print(convertDistanceIfUnit("100km"))
    print(convertDistanceIfUnit("10 km"))
    
    print(convertDistanceIfUnit("1000.8m"))
    
    
test()