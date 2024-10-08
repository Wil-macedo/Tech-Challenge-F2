import calendar

def sumDay(currentDate:str):
    
    year, month, day = map(int, currentDate.split("-"))
    
    _, dayNumber = calendar.monthrange(year, month)  # Pega número de dias do mês atual.

    if dayNumber == day:  # É o último dia tem que somar o mês
        if month == 12:  # Tem que somar o ano !
            # O mês era dezembro, último dia então vira o ano e é primeiro de janeiro.
            year += 1
            month = 1
            day = 1
        
        else:
            month += 1
            day = 1
    else:
        day += 1
    
    if day <= 9:
        day = f"0{day}"
    
    if month <= 9:
        month = f"0{month}"
    
    return f"{year}-{month}-{day}"