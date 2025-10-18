
categories_online = {
    'таргет': 0,
    'звонобот': 1,
    'InstagramBio': -7,
    'InstagramDirect': -6,
    'InstagramManychat': -5,
    'SEO': -8,
    'GOOGLE': -10,
    'TIKTOK': -9,
    'WZ (WABA Аст - 77474224051)': -4,
    'WZ (Аст - 77055153346)': -3,
    'WZ (WABA Алм - 77474223609)': -2,
    'WZ (Алм - 77780879617)': -1,
    'другой город': -11,
    'прочее': 2,
}

categories = {
    'таргет': 0,
    'звонобот': 1,
    'InstagramBio': -7,
    'InstagramDirect': -6,
    'InstagramManychat': -5,
    'SEO': -8,
    'GOOGLE': -10,
    'TIKTOK': -9,
    'WZ (WABA Аст - 77474224051)': -4,
    'WZ (Аст - 77055153346)': -3,
    'WZ (WABA Алм - 77474223609)': -2,
    'WZ (Алм - 77780879617)': -1,
    'прочее': 2,
}

groups = {
    "total": {
        "base": {
            "total_count": {
                "type": "count",
                "title": "Кол-во лидов общее"
            },
            "categories": {
                "fields": [
                    {
                        "type": "count",
                        "prefix": "Кол-во"
                    }
                ]
            }
        }
    },
    "process": {
        "base": {
            "total_count": {
                "type": "count",
                "title": "Кол-во обработанных лидов"
            },
            "categories": {
                "fields": [
                    {
                        "type": "count",
                        "prefix": "Кол-во обработки"
                    },
                    {
                        "type": "ratio",
                        "a": "process",
                        "b": "total",
                        "prefix": "% обработки"
                    },
                ]
            },
            "total_ratio": {
                "type": "ratio",
                "a": "process",
                "b": "total",
                "title": "% обработки"
            }
        }
    },
    "qual": {
        "base": {
            "total_count": {
                "type": "count",
                "title": "Кол-во квал лидов"
            },
            "categories": {
                "fields": [
                    {
                        "type": "count",
                        "prefix": "Кол-во квал"
                    },
                    {
                        "type": "ratio",
                        "a": "qual",
                        "b": "process",
                        "prefix": "Квалификация"
                    },
                ]
            },
            "total_ratio": {
                "type": "ratio",
                "a": "qual",
                "b": "process",
                "title": "% Квалификации"
            }
        }
    },
    "success": {
        "base": {
            "total_count": {
                "type": "count",
                "title": "Кол-во успешек"
            },
            "categories": {
                "fields": [
                    {
                        "type": "count",
                        "prefix": "Кол-во успешек"
                    },
                    {
                        "type": "ratio",
                        "a": "success",
                        "b": "process",
                        "prefix": "% продаж с"
                    },
                    {
                        "type": "ratio",
                        "a": "success",
                        "b": "qual",
                        "prefix": "% продаж с",
                        "postfix": "с квала" 
                    },
                ]
            },
            "total_ratio": {
                "type": "ratio",
                "a": "success",
                "b": "process",
                "title": "Конверсия из лида в продажу"
            },
            "total_ratio_qual": {
                "type": "ratio",
                "a": "success",
                "b": "qual",
                "title": "Конверсия из квал-лида в продажу"
            }
        }
    }
}