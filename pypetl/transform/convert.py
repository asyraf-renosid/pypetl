def none2str(data, string = ''):
    if data == None:
        return string
    else:
        return data

def date2str(data, null = '', datesep = '', datetimesep = '', timesep = ''):
    date = none2str(data)
    if date == '':
        return null
    else:
        return date.strftime(
            '%s%s%s'%(
                datesep.join(repr(d) for d in ['%Y', '%m', '%d']).replace("'", ""),
                datetimesep,
                timesep.join(repr(d) for d in ['%H', '%M', '%S']).replace("'", "")
            )
        )

def num2str(data):
    return str(data)

def data2str(table):
    fields = table.fieldnames()
    for field in fields:
        for type in table.typeset(field): 
            if type in ['datetime', 'date', 'time']:
                table = table.convert(field, lambda rec: date2str(rec, 'null', '-', ' ', ':'))
            if type == 'str':
                table = table.convert(field, lambda rec: none2str(rec, 'null').replace("'", "`")[0:67]+'...' if len(none2str(rec, 'null').replace("'", "`")) >= 70 else none2str(rec, 'null').replace("'", "`"))
            if type in ['int', 'Decimal']:
                table = table.convert(field, lambda rec: str(none2str(rec, 'null')))
            if type == 'NoneType':
                table = table.convert(field, lambda rec: none2str(rec, 'null'))
            if type == 'bool':
                table = table.convert(field, lambda rec: 'True' if rec == True else 'False')
    return table