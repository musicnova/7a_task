package json2schema;

public class Example {
    
    // EXAMPLE http://andjo.free.fr/civ/CombatCalc2.htm
    // BASE https://forums.civfanatics.com/threads/civ4-combat-odds-calculator-excel-spreadsheet.148354/

    // USELESS https://dl.dropbox.com/u/4995277/CombatCalculator.xlsm
    // USELESS https://civilization.fandom.com/wiki/Combat_(Civ5 )

    // MANUAL
    // https://steamcommunity.com/sharedfiles/filedetails/?id=170194443

/*
    Preface
    This Guide utilises the data from the newest expansion of civilization 5, Brave New World. This guide could be relevant to Gods&Kings but definetely not the Base game.

    All of the data, particularly the approximate damage, are obtained in the following settings.
    Difficulty : Prince
    Game Speed: Standard
    Map Size: Standard (8 Civilizations)


    Background
    I am already tired of engaging in a war hoping that lady luck will not fail me because i do not precisely understand how much damage will i cause to my adversaries. If you are an uncivilized warmonger like me, you would want to have some sort of a table or graph that details how much damage that is involved in terms of combat strength ratio.


            Basics
    First and foremost, the damage dealt and recieved in a combat depends on the combat strength ratio of the involved military units. This combat strength is not the "base" combat strength but the combat strength after it is modified by factors such as terrain, promotions and great general bonus.
    The initial health of the military units plays a factor as well in the outcome of the battle and thus, such statistic will be noted in this guide.


            Melee & Ranged Damage Guide
    The data below is obtained via screenshots when military units are about to fight.
    Accoding to my observation, the damage calculation of ranged and melee units are consistent in terms of their combat ratio. therefore we can utilse this table to speculate the damage outcome of both melee and ranged units. Furthermore, please note that the data is approximate and may prone to error.

    How to read the Table
    The combat ratio is the combat strength comparison rate between the attacking unit and the defending unit. The Damage Dealt is the damage dealt to the defending unit and it could be used for melee or ranged combat.

    If the attacking unit combat strength is lower than that of the defending unit, in order to determine the damage dealt:
    reverse the attacking unit and the defending unit combat strength
    calculate its "combat ratio" and found the damage dealt from the table
    multiply the damage dealt with the melee damage ratio as if you are trying to get the damage recieved. (I think it could also be used for ranged combat)
    This damage recieved is your damage dealt
    Combat Ratio and Damage Comparison Table
    Combat Ratio	Damage Dealt	Melee Damage Ratio
1.00: 1	30	1.00: 1
        1.25: 1	33	1.25: 1
        1.33: 1	35	1.33: 1
        1.50: 1	40	2.00: 1
        1.75: 1	45	2.50: 1
        2.00: 1	50	3.00: 1
        2.50: 1	70	5.50: 1
        3.00: 1	90	9.00: 1
        3+ : 1	100+	Flawless

    Example:
    i want to know how much damage my musketman (24 Str) can deal to the enemy's cannon (14 Str). Their Str ratio is 24:14 = 1.75:1 (approximately). According to the table, with this Str ratio i would damage the cannon by 45 damage
    How much damage could my gatling gun(30 Str) deal to the enemy's city with str of 60? Firstly, their Str ratio is 30:60= 1:2. Since my Str is lower than the enemy's, i need to swap the combat ratio into 2:1 and find the damage rate for this str ratio, 3:1. Afterwards, i multiply the damage ratio with damage dealt; 1/3*50= 18. Now, i know that i will deal a mere 18 damage to the city.
    As can be seen from the table, the combat damage rate is not proportionate. Instead, the damage is the square of the combat strength rate; More like
    Damage = (combat strength ratio)^2 * constant.
    In order to get and instant kill, additionally, the combat strength ratio is required to be above 3: 1


    The Health Factor
    It is significantly tough to determine the precise data that connects the relationship between fighting units' remaining health and its damage output. According to the mainstream community, however, for every 1 hit point lost, the damage output of the unit is lowered by 0.5%. Therefore a unit that has 1 hit point remaining will only deal 51% of its damage when it is fully healthy. Moreover, the remaining health influence the damage output and not the overall combat strength, unlike what thecivilopedia has stated.

    As for ranged unit, according to my observation, its damage dealt depends on the units remaining health and NOT the health of the defending unit. The damage output of cities, moreover, is inderpendent of its remaining health.


    Cities
    Cities has an initial hit points of 200 and it will regenerate its hit points by at least 20, depending on its maximum health. Thus, if the city has a wall, its hit points would be 250 with a regeneration rate of 22. The damage output of cities is independent of its remaining hit points.
    A city gets about 50% combat bonus when located in hill and normally, for every population of the city, its strength gets boosted by 0.5... (needs further proof)


    Closing
    All my screenshot are in .tga and it is not supported by Steam. Therefore, no pics guys, sorry about that. Furthermore, I am convinced that my article is not perfect and i would like to improve this article in the near future. Therefore, i am open to feedback and suggestions for this guide.Thank you for your time and I hope you have enjoyed this article.


    Million Dollar Question
    A steam user just post a very interesting question and i must say it is something that we should ponder about. If you solved this question i am going to give you million dollars... in your dream. In general, what is the minimum requirements of a city to withstand an assault of a Giant Death Robot?

    Tips:
    Firstly let us browse all the building fortifications that is normally available to cities (No wall of Babylon).
    Wall (+5 Str, +50 Hp)
    Castle (+7 Str, +25 Hp)
    Arsenal (+9 Str, +25 Hp)
    Military base (+12 Str, +25 Hp)
    Total boost (+33 Str, +125 Hp)

    A city on an open terrain with 1 population, no str enchancing building and no garrison has a combat strength of 8. Therefore, with all the fortifications built, The city possessed a 41 Combat strength with 325 Hit points.
    Not let us take a look at Giant Death Robot. It has a 150 combat strength and the most awesome part of it is that in BNW... it has NO combat penalty against cities.

    Now let us calculate the damage involved in their combat.
    The combat ratio is 150: 41, to simplify: 3+: 1. The damage ratio, is, consequently, somewhere on 10+: 1. So in a combat, the city deal damage lower than 10 while the GDR deal a whopping 100+ DAMAGE. I do not know about you but I think the citizens of that city would have prepared themselved to be ruled by giant machines in 3 turns.

            Practically, therefore, what does it take for a city to withstand an attack from a giant robot?
    Hint: you could consider all city enchancing elements from social policies, wonders and so on.

    The user with the best practical answer (applicable in a normal civ game) would have their name and their answer be posted at the end of this question (you could choose to remain anonymous if you want).
*/
}
