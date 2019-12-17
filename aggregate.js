/**
 * transfer collection - 转账记录
 * -----------------------------
 * timestamp - 转账时间
 * from      - 转账发起人
 * to        - 转账接收人
 * amount    - 转账金额
 */

conversionStage = {
    $project: {
        from: 1,
        to: 1,
        amount: 1,
        timestamp: {
            $convert : {
                input: "$timestamp",
                to: "date",
                onError: {
                    $concat: ["Could not convert ",
                                {$toString: "$timestamp"},
                                " to type date."]
                },
                onNull: "Missing timestamp."
            }
        }
    }
};

filterStage = {
    $match: {
        timestamp: {"$type": "date"}
    }
};

calcStage = {
    $group: {
        _id: {account: "$from", year: {$year: "$timestamp"}, month: {$month: "$timestamp"}},
        sum: {$sum: "$amount"},
        count: {$sum: 1}
    }
};

sortStage = {
    $sort : { "_id.account": 1, "_id.year": 1, "_id.month": 1 }
};

db.transfer.aggregate([conversionStage, filterStage, calcStage]);