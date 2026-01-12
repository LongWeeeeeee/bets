# Stratz MatchPlayer schema (extracted via GraphQL introspection)

Note: Many fields in Stratz schema do not carry descriptions. The table below preserves the schema type info; blank descriptions mean the schema did not provide docs.

## MatchPlayerType (requested fields)
| field | type | description |
| --- | --- | --- |
| matchId | Long |  |
| match | MatchType |  |
| playerSlot | Byte |  |
| steamAccountId | Long |  |
| steamAccount | SteamAccountType |  |
| isRadiant | Boolean |  |
| isVictory | Boolean |  |
| heroId | Short |  |
| gameVersionId | Short |  |
| hero | HeroType |  |
| kills | Byte |  |
| deaths | Byte |  |
| assists | Byte |  |
| leaverStatus | LeaverStatusEnum |  |
| numLastHits | Short |  |
| numDenies | Short |  |
| goldPerMinute | Short |  |
| networth | Int |  |
| experiencePerMinute | Short |  |
| level | Byte |  |
| gold | Int |  |
| goldSpent | Int |  |
| heroDamage | Int |  |
| towerDamage | Int |  |
| heroHealing | Int |  |
| partyId | Byte |  |
| isRandom | Boolean |  |
| lane | MatchLaneType |  |
| position | MatchPlayerPositionType |  |
| streakPrediction | Short |  |
| intentionalFeeding | Boolean |  |
| role | MatchPlayerRoleType |  |
| roleBasic | MatchPlayerRoleType |  |
| imp | Short |  |
| award | MatchPlayerAward |  |
| item0Id | Short |  |
| item1Id | Short |  |
| item2Id | Short |  |
| item3Id | Short |  |
| item4Id | Short |  |
| item5Id | Short |  |
| backpack0Id | Short |  |
| backpack1Id | Short |  |
| backpack2Id | Short |  |
| neutral0Id | Short | The item id of the dedicated neutral item slot (7.24 and after). From game versions 7.23 to 7.24, this was the BackPack3Id (the 4th backpack slot item id). |
| behavior | Short |  |
| stats | MatchPlayerStatsType |  |
| playbackData | MatchPlayerPlaybackDataType |  |
| heroAverage | [HeroPositionTimeDetailType] | Detailed output of data per minute for each hero. |
| additionalUnit | MatchPlayerAdditionalUnitType |  |
| dotaPlus | HeroDotaPlusLeaderboardRankType | Gets the players of Dota which have DotaPlus and have a high level hero. |
| abilities | [PlayerAbilityType] |  |
| invisibleSeconds | Int |  |
| dotaPlusHeroXp | Int |  |
| variant | Byte |  |

## MatchType
| field | type | description |
| --- | --- | --- |
| id | Long |  |
| didRadiantWin | Boolean |  |
| durationSeconds | Int |  |
| startDateTime | Long |  |
| endDateTime | Long |  |
| towerStatusRadiant | Int |  |
| towerStatusDire | Int |  |
| barracksStatusRadiant | Short |  |
| barracksStatusDire | Short |  |
| clusterId | Int |  |
| firstBloodTime | Int |  |
| lobbyType | LobbyTypeEnum |  |
| numHumanPlayers | Int |  |
| gameMode | GameModeEnumType |  |
| replaySalt | Long |  |
| isStats | Boolean |  |
| tournamentId | Int |  |
| tournamentRound | Short |  |
| actualRank | Short |  |
| averageRank | Short |  |
| averageImp | Short |  |
| parsedDateTime | Long |  |
| statsDateTime | Long |  |
| leagueId | Int |  |
| league | LeagueType |  |
| radiantTeamId | Int |  |
| radiantTeam | TeamType |  |
| direTeamId | Int |  |
| direTeam | TeamType |  |
| seriesId | Long |  |
| series | SeriesType |  |
| gameVersionId | Short |  |
| regionId | Byte |  |
| sequenceNum | Long |  |
| rank | Int |  |
| bracket | Byte |  |
| analysisOutcome | MatchAnalysisOutcomeType |  |
| predictedOutcomeWeight | Byte |  |
| players | [MatchPlayerType] |  |
| radiantNetworthLeads | [Int] | This begins at -60 to 0 seconds (Index 0). |
| radiantExperienceLeads | [Int] | This begins at -60 to 0 seconds (Index 0). |
| radiantKills | [Int] | This begins at -60 to 0 seconds (Index 0). |
| direKills | [Int] | This begins at -60 to 0 seconds (Index 0). |
| pickBans | [MatchStatsPickBanType] | This begins at -60 to 0 seconds (Index 0). |
| towerStatus | [MatchStatsTowerReportType] |  |
| laneReport | MatchStatsLaneReportType |  |
| winRates | [Decimal] |  |
| predictedWinRates | [Decimal] |  |
| chatEvents | [MatchStatsChatEventType] |  |
| towerDeaths | [MatchStatsTowerDeathType] |  |
| playbackData | MatchPlaybackDataType |  |
| spectators | [MatchPlayerSpectatorType] |  |
| bottomLaneOutcome | LaneOutcomeEnums |  |
| midLaneOutcome | LaneOutcomeEnums |  |
| topLaneOutcome | LaneOutcomeEnums |  |
| didRequestDownload | Boolean |  |

## HeroType
| field | type | description |
| --- | --- | --- |
| id | Short |  |
| name | String |  |
| displayName | String |  |
| shortName | String |  |
| aliases | [String] |  |
| gameVersionId | Short |  |
| abilities | [HeroAbilityType] |  |
| roles | [HeroRoleType] |  |
| language | HeroLanguageType |  |
| talents | [HeroTalentType] |  |
| facets | [HeroFacetType] |  |
| stats | HeroStatType |  |

## MatchPlayerStatsType
| field | type | description |
| --- | --- | --- |
| matchId | Long |  |
| steamAccountId | Long |  |
| gameVersionId | Short |  |
| level | [Int] |  |
| killEvents | [MatchPlayerStatsKillEventType] |  |
| deathEvents | [MatchPlayerStatsDeathEventType] |  |
| assistEvents | [MatchPlayerStatsAssistEventType] |  |
| lastHitsPerMinute | [Int] |  |
| goldPerMinute | [Int] |  |
| experiencePerMinute | [Int] |  |
| healPerMinute | [Int] |  |
| heroDamagePerMinute | [Int] |  |
| towerDamagePerMinute | [Int] |  |
| towerDamageReport | [MatchPlayerStatsTowerDamageReportType] |  |
| courierKills | [MatchPlayerStatsCourierKillEventType] |  |
| wards | [MatchPlayerStatsWardEventType] |  |
| itemPurchases | [MatchPlayerItemPurchaseEventType] |  |
| itemUsed | [MatchPlayerStatsItemUsedEventType] |  |
| allTalks | [MatchPlayerStatsAllTalkEventType] |  |
| chatWheels | [MatchPlayerStatsChatWheelEventType] |  |
| actionsPerMinute | [Int] |  |
| actionReport | MatchPlayerStatsActionReportType |  |
| locationReport | [MatchPlayerStatsLocationReportType] |  |
| farmDistributionReport | MatchPlayerStatsFarmDistributionReportType |  |
| runes | [MatchPlayerStatsRuneEventType] |  |
| abilityCastReport | [MatchPlayerStatsAbilityCastReportType] |  |
| heroDamageReport | MatchPlayerStatsHeroDamageReportType |  |
| inventoryReport | [MatchPlayerInventoryType] |  |
| networthPerMinute | [Int] |  |
| campStack | [Int] |  |
| matchPlayerBuffEvent | [MatchPlayerStatsBuffEventType] |  |
| deniesPerMinute | [Int] |  |
| impPerMinute | [Int] |  |
| tripsFountainPerMinute | [Int] |  |
| spiritBearInventoryReport | [MatchPlayerSpiritBearInventoryType] |  |
| heroDamageReceivedPerMinute | [Int] |  |
| wardDestruction | [MatchPlayerWardDestuctionObjectType] |  |

## MatchPlayerPlaybackDataType
| field | type | description |
| --- | --- | --- |
| abilityLearnEvents | [AbilityLearnEventsType] |  |
| abilityUsedEvents | [AbilityUsedEventsType] |  |
| abilityActiveLists | [AbilityActiveListType] |  |
| itemUsedEvents | [ItemUsedEventType] |  |
| playerUpdatePositionEvents | [PlayerUpdatePositionDetailType] |  |
| playerUpdateGoldEvents | [PlayerUpdateGoldDetailType] |  |
| playerUpdateAttributeEvents | [PlayerUpdateAttributeDetailType] |  |
| playerUpdateLevelEvents | [PlayerUpdateLevelDetailType] |  |
| playerUpdateHealthEvents | [PlayerUpdateHealthDetailType] |  |
| playerUpdateBattleEvents | [PlayerUpdateBattleDetailType] |  |
| killEvents | [KillDetailType] |  |
| deathEvents | [DeathDetailType] |  |
| assistEvents | [AssistDetailType] |  |
| csEvents | [LastHitDetailType] |  |
| goldEvents | [GoldDetailType] |  |
| experienceEvents | [ExperienceDetailType] |  |
| healEvents | [HealDetailType] |  |
| heroDamageEvents | [HeroDamageDetailType] |  |
| towerDamageEvents | [TowerDamageDetailType] |  |
| inventoryEvents | [InventoryType] |  |
| purchaseEvents | [ItemPurchaseType] |  |
| buyBackEvents | [BuyBackDetailType] |  |
| streakEvents | [StreakEventType] |  |
| runeEvents | [PlayerRuneDetailType] |  |
| spiritBearInventoryEvents | [SpiritBearInventoryType] |  |

## HeroPositionTimeDetailType
| field | type | description |
| --- | --- | --- |
| heroId | Short! |  |
| week | Int! |  |
| time | Int! |  |
| position | MatchPlayerPositionType |  |
| bracketBasicIds | RankBracketBasicEnum |  |
| matchCount | Long |  |
| remainingMatchCount | Long |  |
| winCount | Long |  |
| mvp | Decimal |  |
| topCore | Decimal |  |
| topSupport | Decimal |  |
| courierKills | Decimal |  |
| apm | Decimal |  |
| casts | Decimal |  |
| abilityCasts | Decimal |  |
| kills | Decimal |  |
| deaths | Decimal |  |
| assists | Decimal |  |
| networth | Decimal |  |
| xp | Decimal |  |
| cs | Decimal |  |
| dn | Decimal |  |
| neutrals | Decimal |  |
| heroDamage | Decimal |  |
| towerDamage | Decimal |  |
| physicalDamage | Decimal |  |
| magicalDamage | Decimal |  |
| physicalDamageReceived | Decimal |  |
| magicalDamageReceived | Decimal |  |
| tripleKill | Decimal |  |
| ultraKill | Decimal |  |
| rampage | Decimal |  |
| godLike | Decimal |  |
| goldPerMinute | Decimal |  |
| disableCount | Decimal |  |
| disableDuration | Decimal |  |
| stunCount | Decimal |  |
| stunDuration | Decimal |  |
| slowCount | Decimal |  |
| slowDuration | Decimal |  |
| healingSelf | Decimal |  |
| healingAllies | Decimal |  |
| invisibleCount | Decimal |  |
| runePower | Decimal |  |
| runeBounty | Decimal |  |
| level | Decimal |  |
| campsStacked | Decimal |  |
| supportGold | Decimal |  |
| purgeModifiers | Decimal |  |
| ancients | Decimal |  |
| teamKills | Decimal |  |
| goldLost | Decimal |  |
| goldFed | Decimal |  |
| buybackCount | Decimal |  |
| weakenCount | Decimal |  |
| weakenDuration | Decimal |  |
| physicalItemDamage | Decimal |  |
| magicalItemDamage | Decimal |  |
| healingItemSelf | Decimal |  |
| healingItemAllies | Decimal |  |
| xpFed | Decimal |  |
| pureDamageReceived | Decimal |  |
| attackDamage | Decimal |  |
| attackCount | Decimal |  |
| castDamage | Decimal |  |
| damageReceived | Decimal |  |
| damage | Decimal |  |
| pureDamage | Decimal |  |
| kDAAverage | Decimal |  |
| killContributionAverage | Decimal |  |
| stompWon | Decimal |  |
| stompLost | Decimal |  |
| comeBackWon | Decimal |  |
| comeBackLost | Decimal |  |

## MatchPlayerAdditionalUnitType
| field | type | description |
| --- | --- | --- |
| item0Id | Short |  |
| item1Id | Short |  |
| item2Id | Short |  |
| item3Id | Short |  |
| item4Id | Short |  |
| item5Id | Short |  |
| backpack0Id | Short |  |
| backpack1Id | Short |  |
| backpack2Id | Short |  |
| neutral0Id | Short |  |

## HeroDotaPlusLeaderboardRankType
| field | type | description |
| --- | --- | --- |
| heroId | Short |  |
| steamAccountId | Long |  |
| level | Byte |  |
| totalActions | Long |  |
| createdDateTime | Long |  |
| steamAccount | SteamAccountType |  |

## PlayerAbilityType
| field | type | description |
| --- | --- | --- |
| abilityId | Int! |  |
| time | Int! |  |
| level | Int! |  |
| gameVersionId | Short |  |
| abilityType | AbilityType |  |
| isTalent | Boolean |  |