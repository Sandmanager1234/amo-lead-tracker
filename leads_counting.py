import os
from models_new import LeadsResponse


def get_total(leads: LeadsResponse):
    total = {
        "all": 0,
        "target": 0,
        "other_city": 0,
        "zvonobot": 0,
        "other": 0,
    }
    
    for lead in leads.leads:
        total["all"] += 1
        for tag in lead.tags:
            if tag.type == 'target':
                total["target"] += 1
            elif tag.type == 'zvonobot':
                total["zvonobot"] += 1
            elif tag.type == 'other':
                total["other"] += 1
            elif tag.type == 'other_city':
                total["other_city"] += 1

    return total

def get_processed(leads: LeadsResponse):
    total_processed = {
        "all": 0,
        "target": 0,
        "other_city": 0,
        "zvonobot": 0,
        "other": 0
    }

    for lead in leads.leads:
        if lead.is_after_processing:
            total_processed["all"] += 1
            for tag in lead.tags:
                if tag.type == 'target':
                    total_processed["target"] += 1
                elif tag.type == 'zvonobot':
                    total_processed["zvonobot"] += 1
                elif tag.type == 'other':
                    total_processed["other"] += 1
                elif tag.type == 'other_city':
                    total_processed["other_city"] += 1
    
    return total_processed

def get_qualified(leads: LeadsResponse):
    total_qualified = {
        "all": 0,
        "target": 0,
        "other_city": 0,
        "zvonobot": 0,
        "other": 0,
    }

    for lead in leads.leads:
        if lead.is_qualified:
            total_qualified["all"] += 1
            for tag in lead.tags:
                if tag.type == 'target':
                    total_qualified["target"] += 1
                elif tag.type == 'zvonobot':
                    total_qualified["zvonobot"] += 1
                elif tag.type == 'other':
                    total_qualified["other"] += 1
                elif tag.type == 'other_city':
                    total_qualified["other_city"] += 1
    
    return total_qualified

def get_success(leads: LeadsResponse):
    total_success = {
        "all": 0,
        "target": 0,
        "zvonobot": 0,
        "other": 0,
        "other_city": 0
    }

    for lead in leads.leads:
        if lead.is_success:
            total_success["all"] += 1
            for tag in lead.tags:
                if tag.type == 'target':
                    total_success["target"] += 1
                elif tag.type == 'zvonobot':
                    total_success["zvonobot"] += 1
                elif tag.type == 'other':
                    total_success["other"] += 1
                elif tag.type == 'other_city':
                    total_success["other_city"] += 1
    
    return total_success