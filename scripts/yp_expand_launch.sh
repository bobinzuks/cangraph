#!/bin/bash
# Launch comprehensive YellowPages.ca scrape across ALL Canadian business categories x major cities
# Also extracts phones, emails, domains

set -e

# 200+ business trades/categories covering ALL of Canadian commerce
TRADES=(
    # Home services & construction
    plumber electrician roofer hvac landscaping painter flooring drywall
    carpenter locksmith pest-control cleaning-services handyman contractor
    renovation windows-doors fence concrete paving excavation welding
    insulation demolition masonry tile-installation cabinet-maker
    deck-builder foundation-repair waterproofing siding gutters
    garage-door pool-service solar-installer fireplace-chimney
    snow-removal tree-service appliance-repair home-inspection

    # Automotive
    auto-repair towing car-dealer car-rental auto-body auto-parts
    tire-shop oil-change car-wash motorcycle-repair boat-repair
    rv-dealer truck-repair auto-detailing brake-service transmission

    # Health & medical
    dentist doctor chiropractor massage-therapy physiotherapist
    optometrist pharmacy psychologist naturopath osteopath acupuncture
    veterinarian dermatologist orthodontist podiatrist audiologist
    nutrition-dietitian fertility-clinic cosmetic-surgery medical-clinic
    walk-in-clinic hospital laboratory diagnostic-imaging

    # Professional services
    accountant lawyer notary financial-advisor insurance-broker
    mortgage-broker real-estate tax-preparation bookkeeping
    consulting human-resources translation immigration-consultant
    architect engineer surveyor interior-designer graphic-designer
    marketing-agency web-design advertising public-relations

    # Beauty & personal care
    hair-salon barber nail-salon spa tanning-salon tattoo eyebrow
    cosmetology makeup-artist esthetician skin-care waxing

    # Food & hospitality
    restaurant caterer bakery butcher grocery-store bar pub cafe
    coffee-shop fast-food pizzeria diner food-truck juice-bar
    ice-cream hotel motel bed-breakfast event-venue banquet-hall
    nightclub brewery winery distillery

    # Retail
    clothing-store shoe-store jewellery furniture-store
    electronics-store hardware-store toy-store book-store
    pet-store florist gift-shop thrift-store pawn-shop
    sporting-goods bicycle-shop music-store art-gallery
    antique-store craft-store liquor-store convenience-store
    department-store mall

    # Fitness & recreation
    gym yoga-studio pilates martial-arts dance-studio swimming-pool
    golf-course bowling skating-rink trampoline-park climbing-gym
    crossfit personal-trainer sports-club recreation-center

    # Education
    daycare preschool tutoring driving-school music-lessons
    language-school private-school college university
    vocational-training childcare babysitter summer-camp

    # Entertainment & events
    photographer videographer dj event-planner wedding-planner
    party-rental bouncy-castle karaoke escape-room arcade
    movie-theatre theatre museum zoo aquarium amusement-park

    # Business services
    courier printing-services sign-maker packaging storage
    security-service staffing-agency recruitment answering-service
    call-center debt-collection mailbox-rental office-rental
    coworking virtual-office business-coach

    # Technology
    computer-repair it-services cell-phone-repair smartphone-repair
    network-installation software-development data-recovery
    cybersecurity telecom internet-provider

    # Miscellaneous
    funeral-home crematorium cemetery monument religious
    charity non-profit government community-center library
    tailor alterations dry-cleaner laundromat shoe-repair
    upholstery carpet-cleaning janitorial window-cleaning
    septic-service waste-management recycling scrap-metal
    farm agriculture nursery garden-center tree-farm
    travel-agency tour-operator cruise-line airline-service
    moving-services freight-transport warehouse logistics
)

# ALL Canadian cities with meaningful business populations (population >20K)
CITIES=(
    # Ontario (major + mid-sized)
    "Toronto:ON" "Ottawa:ON" "Mississauga:ON" "Brampton:ON" "Hamilton:ON"
    "London:ON" "Markham:ON" "Vaughan:ON" "Kitchener:ON" "Windsor:ON"
    "Richmond-Hill:ON" "Oakville:ON" "Burlington:ON" "Oshawa:ON" "Barrie:ON"
    "Sudbury:ON" "Kingston:ON" "Guelph:ON" "Thunder-Bay:ON" "St-Catharines:ON"
    "Cambridge:ON" "Whitby:ON" "Ajax:ON" "Waterloo:ON" "Milton:ON"
    "Niagara-Falls:ON" "Newmarket:ON" "Peterborough:ON" "Sarnia:ON" "Belleville:ON"
    "Sault-Ste-Marie:ON" "Welland:ON" "North-Bay:ON" "Cornwall:ON" "Chatham:ON"
    "Brantford:ON" "Pickering:ON" "Caledon:ON" "Georgina:ON" "Clarington:ON"

    # Quebec
    "Montreal:QC" "Quebec-City:QC" "Laval:QC" "Gatineau:QC" "Longueuil:QC"
    "Sherbrooke:QC" "Saguenay:QC" "Levis:QC" "Trois-Rivieres:QC"
    "Terrebonne:QC" "Saint-Jean-sur-Richelieu:QC" "Repentigny:QC"
    "Drummondville:QC" "Saint-Jerome:QC" "Granby:QC" "Blainville:QC"
    "Brossard:QC" "Chateauguay:QC" "Saint-Hyacinthe:QC" "Shawinigan:QC"
    "Dollard-des-Ormeaux:QC" "Rimouski:QC" "Victoriaville:QC"
    "Rouyn-Noranda:QC" "Sorel-Tracy:QC"

    # British Columbia
    "Vancouver:BC" "Surrey:BC" "Burnaby:BC" "Richmond:BC" "Victoria:BC"
    "Abbotsford:BC" "Coquitlam:BC" "Kelowna:BC" "Langley:BC" "Saanich:BC"
    "Delta:BC" "Kamloops:BC" "Nanaimo:BC" "Chilliwack:BC" "Prince-George:BC"
    "Maple-Ridge:BC" "New-Westminster:BC" "North-Vancouver:BC" "Vernon:BC"
    "White-Rock:BC" "Port-Coquitlam:BC" "West-Vancouver:BC" "Mission:BC"
    "Penticton:BC" "Campbell-River:BC" "Courtenay:BC" "Cranbrook:BC"

    # Alberta
    "Calgary:AB" "Edmonton:AB" "Red-Deer:AB" "Lethbridge:AB" "St-Albert:AB"
    "Medicine-Hat:AB" "Grande-Prairie:AB" "Airdrie:AB" "Spruce-Grove:AB"
    "Okotoks:AB" "Fort-McMurray:AB" "Leduc:AB" "Cochrane:AB" "Camrose:AB"
    "Lloydminster:AB" "Brooks:AB" "Wetaskiwin:AB"

    # Manitoba
    "Winnipeg:MB" "Brandon:MB" "Steinbach:MB" "Thompson:MB" "Portage-la-Prairie:MB"
    "Winkler:MB" "Selkirk:MB" "Morden:MB" "Dauphin:MB"

    # Saskatchewan
    "Saskatoon:SK" "Regina:SK" "Prince-Albert:SK" "Moose-Jaw:SK"
    "Swift-Current:SK" "Yorkton:SK" "North-Battleford:SK" "Estevan:SK"

    # Nova Scotia
    "Halifax:NS" "Sydney:NS" "Dartmouth:NS" "Truro:NS" "New-Glasgow:NS"
    "Glace-Bay:NS" "Kentville:NS" "Amherst:NS" "Bridgewater:NS" "Yarmouth:NS"

    # New Brunswick
    "Moncton:NB" "Saint-John:NB" "Fredericton:NB" "Dieppe:NB"
    "Riverview:NB" "Miramichi:NB" "Edmundston:NB" "Bathurst:NB"

    # Newfoundland
    "St-Johns:NL" "Corner-Brook:NL" "Mount-Pearl:NL" "Grand-Falls:NL"
    "Gander:NL" "Conception-Bay-South:NL"

    # PEI & Territories
    "Charlottetown:PE" "Summerside:PE" "Stratford:PE"
    "Whitehorse:YT" "Yellowknife:NT" "Iqaluit:NU"
)

echo "=== YellowPages.ca Full Scrape ==="
echo "Trades: ${#TRADES[@]}"
echo "Cities: ${#CITIES[@]}"
echo "Total searches: $((${#TRADES[@]} * ${#CITIES[@]}))"
echo ""
echo "Running on VPS..."

# Run with expanded lists
cd /opt/cangraph
python3 scripts/yellowpages_scraper.py \
    --trades "${TRADES[@]}" \
    --cities "${CITIES[@]}" \
    --pages 1 \
    2>&1 | tee logs/yp_full_$(date +%Y%m%d_%H%M).log
