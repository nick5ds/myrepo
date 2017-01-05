import pandas as pd


keys='Aloha Bowl,Bento Bowl,Burrito Bowl,Cantina Kale Salad,Falafel & Harissa,Smokehouse Salad'
values='true,true,true,true,true,true'
values='1,1,1,1,1,1'
all_keys='Almond Amaretto Coffee,Aloha Bowl,Autumn Spice Coffee,Bento Bowl,Berry Chia Parfait,Breakfast Tea,Build-A-Bowl,Burrito Bowl,Cantina Kale Salad,Chai Coffee,Chili con Quinoa,Chilled Quinoa,Chips & Guacamole,Chips and Guacamole,Cinnamon Cardamom Coffee,Crystal Geyser Sparkling Water,Dark Chocolate Coffee,Dark Roast Coffee,Falafel & Harissa,Goat Cheese & Veggie,Goat Cheese & Veggie Scramble,Goat Cheese and Veggie,Green Sheep Bottled Water,Green Sheep Sparkling Water,Green Spring Jade Tea,Hazelnut Coffee,Herbal Peppermint Leaf Tea,House Potato Chips,House Water,Hummus & Falafel Bowl,Hummus & Pita Chips,Just Greens,Light Roast Coffee,Mediterranean Scramble,Medium Roast Coffee,No Worry Curry,Orange Vanilla Bergamot Coffee,Parfait,Perfect Pearing Salad,Quinoa "Oatmeal",Salad Greens,Savory Stuffing Bowl,Scramble,Seasonal Fresh Fruit,Served Cold,Served Hot,Smart Water,Smokehouse Salad,Southwestern Scramble,Spice Market Bowl,Spiced Apple Quinoa,The Mediterranean,Toasted Coconut Coffee,Toscana Bowl,Tres Chiles,Warm Quinoa,Yogurt Quinoa Parfait,eatsa Mango Peach Tea,eatsa Sparkling Citrus,eatsa Sparkling Cucumber Melon,eatsa Sparkling Ginger Lime,eatsa Sparkling Mango Guava,eatsa Tea'

colnames=all_keys.split(',')
keys=keys.split(',')
values=values.split(',')
    
    

df=pd.DataFrame(columns=colnames,index=[1])
for i,v in enumerate(keys):
    print v 
    df.set_value(1,v,str(values[i]))

#b = '\n'.join(','.join('%0.3f' %x for x in y) for y in df.values)
df.apply(str)#.apply(','.join)
b = '\n'.join(','.join('%s' %x for x in y) for y in df.values)
print b
