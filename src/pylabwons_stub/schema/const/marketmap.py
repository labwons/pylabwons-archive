from pylabwons import DataDict


COLORS = DataDict(
    BLUE2RED = [
        (24, 97, 168), #1861A8
        (34, 139, 230), #228BE6
        (116, 192, 252), #74C0FC
        (168, 168, 168), #A6A6A6
        (255, 135, 135), #FF8787
        (240, 62, 62), #F03E3E
        (201, 42, 42) #C92A2A
    ],
    RED2BLUE = [
        (201, 42, 42), #C92A2A
        (240, 62, 62), #F03E3E
        (255, 135, 135), #FF8787
        (168, 168, 168), #A6A6A6
        (116, 192, 252), #74C0FC
        (34, 139, 230), #228BE6
        (24, 97, 168) #1861A8
    ],
    RED2GREEN = [
        (246, 53, 56), #F63538
        (191, 64, 69), #BF4045
        (139, 68, 78), #8B444E
        (65, 69, 84), #414554
        (53, 118, 78), #35764E
        (47, 158, 79), #2F9E4F
        (48, 204, 90) #30CC5A
    ],
    GREEN2RED = [
        (48, 204, 90), #30CC5A
        (47, 158, 79), #2F9E4F
        (53, 118, 78), #35764E
        (65, 69, 84), #414554
        (139, 68, 78), #8B444E
        (191, 64, 69), #BF4045
        (246, 53, 56) #F63538
    ],
)

MARKETMAP = DataDict(

    returnOn1Day=DataDict(
        method='weighted',
        scale=[-3, -2, -1, 0, 1, 2, 3],
        color='BLUE2RED',
        index=3,
        iconMax='bi-graph-up-arrow',
        iconMin='bi-graph-down-arrow',
        # map-attribute
    ),
    returnOn1Week=DataDict(
        method='weighted',
        scale=[-6, -4, -2, 0, 2, 4, 6],
        color='BLUE2RED',
        index=3,
        iconMax='bi-graph-up-arrow',
        iconMin='bi-graph-down-arrow',
        # map-attribute
    ),
    returnOn1Month=DataDict(
        method='weighted',
        scale=[-10, -6.7, -3.3, 0, 3.3, 6.7, 10],
        color='BLUE2RED',
        index=3,
        iconMax='bi-graph-up-arrow',
        iconMin='bi-graph-down-arrow',
        # map-attribute
    ),
    returnOn3Months=DataDict(
        method='weighted',
        scale=[-18, -12, -6, 0, 6, 12, 18],
        color='BLUE2RED',
        index=3,
        iconMax='bi-graph-up-arrow',
        iconMin='bi-graph-down-arrow',
        # map-attribute
    ),
    returnOn6Months=DataDict(
        method='weighted',
        scale=[-24, -16, -8, 0, 8, 16, 24],
        color='BLUE2RED',
        index=3,
        iconMax='bi-graph-up-arrow',
        iconMin='bi-graph-down-arrow',
        # map-attribute
    ),
    returnOn1Year=DataDict(
        method='weighted',
        scale=[-30, -20, -10, 0, 10, 20, 30],
        color='BLUE2RED',
        index=3,
        iconMax='bi-graph-up-arrow',
        iconMin='bi-graph-down-arrow',
        # map-attribute
    ),
    fiftyTwoWeekHighPct=DataDict(
        method='weighted',
        scale=[-45, -30, -15, 0, 0, 0, 0],
        color='BLUE2RED',
        index=3,
        iconMax='bi-graph-up-arrow',
        iconMin='bi-graph-down-arrow',
        # map-attribute
    ),
    fiftyTwoWeekLowPct=DataDict(
        method='weighted',
        scale=[0, 0, 0, 0, 15, 30, 45],
        color='BLUE2RED',
        index=3,
        iconMax='bi-graph-up-arrow',
        iconMin='bi-graph-down-arrow',
        # map-attribute
    ),
    targetPricePct=DataDict(
        method='weighted',
        scale=[-20, -10, -5, 0, 5, 10, 20],
        color='GREEN2RED',
        index=3,
        iconMax='bi-graph-up-arrow',
        iconMin='bi-graph-down-arrow',
        # map-attribute
    ),

    foreignRate=DataDict(
        method='arithmetic',
        scale=[0, 0, 0, 0, 20, 40, 60],
        color='RED2GREEN',
        index=3,
        iconMax='bi-person-up',
        iconMin='bi-person-down',
        # map-attribute
    ),
    beta=DataDict(
        method='arithmetic',
        scale=[0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2],
        color='RED2GREEN',
        index=3,
        iconMax='bi-graph-up-arrow',
        iconMin='bi-graph-down-arrow',
        # map-attribute
    ),
    yoyRevenue=DataDict(
        method='arithmetic',
        scale=[-30, -20, -10, 0, 10, 20, 30],
        color='RED2GREEN',
        index=3,
        iconMax='bi-building-up',
        iconMin='bi-building-down',
        # map-attribute
    ),
    yoyProfit=DataDict(
        method='arithmetic',
        scale=[-120, -80, -40, 0, 40, 80, 120],
        color='RED2GREEN',
        index=3,
        iconMax='bi-database-up',
        iconMin='bi-database-down',
        # map-attribute
    ),
    yoyEps=DataDict(
        method='arithmetic',
        scale=[-90, -60, -30, 0, 30, 60, 90],
        color='RED2GREEN',
        index=3,
        iconMax='bi-graph-up-arrow',
        iconMin='bi-graph-down-arrow',
        # map-attribute
    ),
    trailingPs=DataDict(
        method='arithmetic',
        scale=[0.5, 2, 3.5, 5, 6.5, 8, 9.5],
        color='GREEN2RED',
        index=3,
        iconMax='bi-arrow-up-square',
        iconMin='bi-arrow-down-square',
        # map-attribute
    ),
    trailingPe=DataDict(
        method='arithmetic',
        scale=[5, 10, 20, 30, 40, 50, 60],
        color='GREEN2RED',
        index=3,
        iconMax='bi-arrow-up-square',
        iconMin='bi-arrow-down-square',
        # map-attribute
    ),
    trailingProfitRate=DataDict(
        method='arithmetic',
        scale=[-15, -10, -5, 0, 5, 10, 15],
        color='RED2GREEN',
        index=3,
        iconMax='bi-building-up',
        iconMin='bi-building-down',
        # map-attribute
    ),
    estimatedRevenueGrowth=DataDict(
        method='arithmetic',
        scale=[-10, -5, 0, 5, 10, 15, 20],
        color='RED2GREEN',
        index=3,
        iconMax='bi-building-up',
        iconMin='bi-building-down',
        # map-attribute
    ),
    estimatedProfitRate=DataDict(
        method='arithmetic',
        scale=[-15, -10, -5, 0, 5, 10, 15],
        color='RED2GREEN',
        index=3,
        iconMax='bi-database-up',
        iconMin='bi-database-down',
        # map-attribute
    ),
    estimatedProfitGrowth=DataDict(
        method='arithmetic',
        scale=[-50, -25, 0, 25, 50, 75, 100],
        color='RED2GREEN',
        index=3,
        iconMax='bi-database-up',
        iconMin='bi-database-down',
        # map-attribute
    ),
    estimatedEpsGrowth=DataDict(
        method='arithmetic',
        scale=[-50, -25, 0, 25, 50, 75, 100],
        color='RED2GREEN',
        index=3,
        iconMax='bi-graph-up-arrow',
        iconMin='bi-graph-down-arrow',
        # map-attribute
    ),
    forwardPe=DataDict(
        method='arithmetic',
        scale=[5, 10, 20, 30, 40, 50, 60],
        color='GREEN2RED',
        index=3,
        iconMax='bi-arrow-up-square',
        iconMin='bi-arrow-down-square',
        # map-attribute
    ),
)

if __name__ == "__main__":
    from pylabwons_stub.schema.const.baseline import BASELINE

    for k in MARKETMAP:
        if not k in BASELINE:
            print(k)