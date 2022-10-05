import scrapy
from scrapy.crawler import CrawlerProcess
import json
import requests
import openpyxl
import mysql.connector
import datetime
import  os


mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password='root',
    database="investors"
)

mycursor = mydb.cursor()

pre_seed = []
seed = []
series_A = []
series_B = []
Location = []
under_represented_founder = []

work_sheet = openpyxl.load_workbook('Meta.xlsx')

for cell in work_sheet['Sheet1']:
    try:
        try:
            value = cell[0].hyperlink.target
        except:
            pass
        try:
            value1 = cell[1].hyperlink.target
        except:
            pass
        try:
            value2 = cell[2].hyperlink.target
        except:
            pass
        try:
            value3 = cell[3].hyperlink.target
        except:
            pass
        try:
            value4 = cell[4].hyperlink.target
        except:
            pass
        try:
            value5 = cell[5].hyperlink.target
        except:
            pass
        try:
            if (value!=None ) or (value!=''):
                pre_seed.append(value)
        except:
            pass

        try:
            if (value1!=None ) or (value1!=''):
                seed.append(value1)
        except:
            pass

        try:
            if (value2!=None ) or (value2!=''):
                series_A.append(value2)
        except:
            pass
        try:
            if (value3!=None ) or (value3!=''):
                series_B.append(value3)
        except:
            pass
        try:
            if (value4!=None ) or (value4!=''):
                Location.append(value4)
        except:
            pass

        try:
            if (value5!=None ) or (value5!=''):
                under_represented_founder.append(value5)
        except:
            pass
    except AttributeError:
        pass

main_list = []

cwd = os.getcwd()
class investor(scrapy.Spider):
    name = 'investor'
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': False,
        'ROBOTSTXT_OBEY': False,
        'CONCURRENT_REQUESTS': 2,
        'DOWNLOAD_TIMEOUT': 10,
        'RETRY_TIMES': 30,
        'RETRY_HTTP_CODES': [302, 503, 400, 403],
        # 'Handle_httpstatus_list': [400, 403],
        'ROTATING_PROXY_LIST_PATH': f'{cwd}/proxy.txt',
        'DOWNLOADER_MIDDLEWARES': {
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            'rotating_proxies.middlewares.BanDetectionMiddleware': 620
        },
    }

    def start_requests(self):
        for prese in list(set(pre_seed)):
            yield scrapy.Request(url=prese)
        for sed in list(set(seed)):
            yield scrapy.Request(url=sed)
        for serA in list(set(series_A)):
            yield scrapy.Request(url=serA)

        for serB in list(set(series_B)):
            yield scrapy.Request(url=serB)
        #
        for loc in list(set(Location)):
            yield scrapy.Request(url=loc)

        for unrefo in list(set(under_represented_founder)):
            yield scrapy.Request(url=unrefo)


    def parse(self,response):

        p_see = response.css('div#stage-pre_seed p.f6.ttu.fw6 ::text').get()
        if p_see == None:
            p_see = ''

        s_see = response.css('div#stage-seed p.f6.ttu.fw6 ::text').get()
        if s_see == None:
            s_see = ''

        s_see_A = response.css('div#stage-series_a p.f6.ttu.fw6 ::text').get()
        if s_see_A == None:
            s_see_A = ''

        s_see_B = response.css('div#stage-series_b p.f6.ttu.fw6 ::text').get()
        if s_see_B == None:
            s_see_B = ''


        if ('Pre-Seed' in p_see) or ('Seed' in s_see) or ('Series A' in s_see_A) or ('Series B' in s_see_B) :
            country_preseed = ['https://signal.nfx.com'+v for v in response.css('div#stage-pre_seed ul li a ::attr(href)').extract()]
            for country_preseed_link in country_preseed:
                yield scrapy.Request(url=country_preseed_link,callback=self.preseed_func)
            country_seed = ['https://signal.nfx.com'+v for v in response.css('div#stage-seed ul li a ::attr(href)').extract()]
            for country_seed_link in country_seed:
                yield scrapy.Request(url=country_seed_link,callback=self.preseed_func)

            country_seriesA = ['https://signal.nfx.com' + v for v in response.css('div#stage-series_a ul li a ::attr(href)').extract()]
            for country_seriesA_link in country_seriesA:
                yield scrapy.Request(url=country_seriesA_link,callback=self.preseed_func)
            country_seriesB = ['https://signal.nfx.com' + v for v in response.css('div#stage-series_b ul li a ::attr(href)').extract()]
            for country_seriesB_link in country_seriesB:
                yield scrapy.Request(url=country_seriesB_link,callback=self.preseed_func)
        else:
            scripst_tags = response.css('script ::text').extract()
            json_resp = json.loads(
                [v for v in scripst_tags if 'window.__APOLLO_STATE__' in v][0].split('window.__APOLLO_STATE__ = ')[-1])
            type_seed = json_resp[list(json_resp.keys())[0]]['slug']
            investor_count = json_resp[list(json_resp.keys())[0]]['investor_count']
            end_cursor = \
            json_resp[f'${list(json_resp.keys())[0]}' + '.scored_investors({"after":null,"first":8}).pageInfo'][
                'endCursor']
            investors_urls = ['https://signal.nfx.com' + v for v in
                              response.css('a.vc-search-card-name ::attr(href)').extract()]

            while len(investors_urls) <= investor_count:

                payload = {"operationName": "vclInvestors",
                           "variables": {"slug": f"{type_seed}", "order": [{}], "after": f"{end_cursor}"},
                           "query": "query vclInvestors($slug: String!, $after: String) {\n  list(slug: $slug) {\n    id\n    slug\n    investor_count\n    vertical {\n      id\n      display_name\n      kind\n      __typename\n    }\n    location {\n      id\n      display_name\n      __typename\n    }\n    stage\n    firms {\n      id\n      name\n      slug\n      __typename\n    }\n    scored_investors(first: 8, after: $after) {\n      pageInfo {\n        hasNextPage\n        hasPreviousPage\n        endCursor\n        __typename\n      }\n      record_count\n      edges {\n        node {\n          ...investorListInvestorProfileFields\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment investorListInvestorProfileFields on InvestorProfile {\n  id\n  person {\n    id\n    first_name\n    last_name\n    name\n    slug\n    is_me\n    is_on_target_list\n    __typename\n  }\n  image_urls\n  position\n  min_investment\n  max_investment\n  target_investment\n  is_preferred_coinvestor\n  firm {\n    id\n    name\n    slug\n    __typename\n  }\n  investment_locations {\n    id\n    display_name\n    location_investor_list {\n      id\n      slug\n      __typename\n    }\n    __typename\n  }\n  investor_lists {\n    id\n    stage_name\n    slug\n    vertical {\n      id\n      display_name\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n"}
                headers = {
                    "authority": "signal-api.nfx.com",
                    "accept": "*/*",
                    "accept-language": "en-US,en;q=0.9,de;q=0.8",
                    "content-type": "application/json",
                    "origin": "https://signal.nfx.com",
                    "referer": "https://signal.nfx.com/",
                    "sec-ch-ua": "\"Google Chrome\";v=\"105\", \"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"105\"",
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": "\"Windows\"",
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-site",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"
                }

                graphql_response = json.loads(
                    requests.post(url='https://signal-api.nfx.com/graphql', data=json.dumps(payload),
                                  headers=headers).text)

                end_cursor = graphql_response['data']['list']['scored_investors']['pageInfo']['endCursor']

                for api_loop in graphql_response['data']['list']['scored_investors']['edges']:
                    investors_urls.append('https://signal.nfx.com/investors/' + api_loop['node']['person']['slug'])

            for investors_ur in list(set(investors_urls)):
                # yield scrapy.Request(url='https://signal.nfx.com/investors/james-wang', callback=self.data_page)
                yield scrapy.Request(url=investors_ur, callback=self.data_page)

        a=2

    def preseed_func(self,response):

        if response.css('main h4 ::text').get() == "This list hasn't launched yet because there are less than 5 investors.":
            pass
        else:
            scripst_tags1 = response.css('script ::text').extract()
            json_resp1 = json.loads([v for v in scripst_tags1 if 'window.__APOLLO_STATE__' in v][0].split('window.__APOLLO_STATE__ = ')[-1])
            type_seed1 = json_resp1[list(json_resp1.keys())[0]]['slug']
            investor_count1 = json_resp1[list(json_resp1.keys())[0]]['investor_count']
            end_cursor1 =  json_resp1[f'${list(json_resp1.keys())[0]}'+'.scored_investors({"after":null,"first":8}).pageInfo']['endCursor']
            investors_urls1 = ['https://signal.nfx.com'+v for v in response.css('a.vc-search-card-name ::attr(href)').extract()]

            while len(investors_urls1) <= investor_count1:

                payload1 = {"operationName": "vclInvestors",
                           "variables": {"slug": f"{type_seed1}", "order": [{}], "after": f"{end_cursor1}"},
                           "query": "query vclInvestors($slug: String!, $after: String) {\n  list(slug: $slug) {\n    id\n    slug\n    investor_count\n    vertical {\n      id\n      display_name\n      kind\n      __typename\n    }\n    location {\n      id\n      display_name\n      __typename\n    }\n    stage\n    firms {\n      id\n      name\n      slug\n      __typename\n    }\n    scored_investors(first: 8, after: $after) {\n      pageInfo {\n        hasNextPage\n        hasPreviousPage\n        endCursor\n        __typename\n      }\n      record_count\n      edges {\n        node {\n          ...investorListInvestorProfileFields\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment investorListInvestorProfileFields on InvestorProfile {\n  id\n  person {\n    id\n    first_name\n    last_name\n    name\n    slug\n    is_me\n    is_on_target_list\n    __typename\n  }\n  image_urls\n  position\n  min_investment\n  max_investment\n  target_investment\n  is_preferred_coinvestor\n  firm {\n    id\n    name\n    slug\n    __typename\n  }\n  investment_locations {\n    id\n    display_name\n    location_investor_list {\n      id\n      slug\n      __typename\n    }\n    __typename\n  }\n  investor_lists {\n    id\n    stage_name\n    slug\n    vertical {\n      id\n      display_name\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n"}
                headers1 = {
                    "authority": "signal-api.nfx.com",
                    "accept": "*/*",
                    "accept-language": "en-US,en;q=0.9,de;q=0.8",
                    "content-type": "application/json",
                    "origin": "https://signal.nfx.com",
                    "referer": "https://signal.nfx.com/",
                    "sec-ch-ua": "\"Google Chrome\";v=\"105\", \"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"105\"",
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": "\"Windows\"",
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-site",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"
                }


                graphql_response1 = json.loads(requests.post(url='https://signal-api.nfx.com/graphql', data=json.dumps(payload1), headers=headers1).text)

                end_cursor1 = graphql_response1['data']['list']['scored_investors']['pageInfo']['endCursor']

                for api_loop1 in graphql_response1['data']['list']['scored_investors']['edges']:
                    investors_urls1.append('https://signal.nfx.com/investors/'+api_loop1['node']['person']['slug'])

            for investors_ur1  in list(set(investors_urls1)):
                yield scrapy.Request(url=investors_ur1,callback=self.data_page)
                # yield scrapy.Request(url='https://signal.nfx.com/investors/satya-patel',callback=self.data_page)
                # yield scrapy.Request(url='https://signal.nfx.com/investors/tasso-argyros',callback=self.data_page)


    def data_page(self,repsonse):
        item = dict()
        item['url'] = repsonse.url
        try:
            item['name'] = repsonse.css('div.relative.identity-block div h1 ::text').get()
        except:
            item['name'] = ''
        try:
            item['image'] = repsonse.css('img.contact-card-img.pull-right ::attr(src)').get()
        except:
            item['image'] = ''


        try:
            item['Type'] = ' '.join(repsonse.css('div#vc-profile h3.subheader.white-subheader.b.pb1 > span ::text').extract())
        except:
            item['Type'] = ''
        try:
            item['firm'] = repsonse.css('h3.subheader.lower-subheader.pb2 ::text').get()
        except:
            item['firm'] = ''

        try:
            item['location'] = repsonse.css('div.subheader.lower-subheader span.nowrap span.ml1 ::text').get()
        except:
            item['location'] = ''

        try:
            item['website'] = repsonse.css('a.ml1.subheader.lower-subheader ::attr(href)').get()
        except:
            item['website'] = ''

        try:
            for invt in repsonse.css('div.sn-margin-top-30.col-sm-3.col-xs-12 > div'):
                if 'Investors who invest with' in invt.css('p.section-label ::text').get():
                    item['Invest With Investor'] = []
                    for netwo in invt.css('div.network-row.sn-small-font'):
                        invest_network = dict()
                        try:
                            invest_network['name'] = netwo.css('a.network-row-investor-name ::text').get()
                        except:
                            invest_network['name'] = ''
                        try:
                            invest_network['image'] = netwo.css('img ::attr(src)').get()
                        except:
                            invest_network['image'] = ''
                        try:
                            invest_network['company'] = netwo.css('a.network-row-firm-name ::text').get()
                        except:
                            invest_network['company'] = ''
                        try:
                            invest_network['url'] = 'https://signal.nfx.com'+netwo.css('a.network-row-investor-name ::attr(href)').get()
                        except:
                            invest_network['url'] = ''

                        item['Invest With Investor'].append(invest_network)
                if 'Scouts & Angels Affiliated With' in invt.css('p.section-label ::text').get():
                    item['Scouts & Angels Affiliated'] = []
                    for netwo in invt.css('div.network-row.sn-small-font'):
                        invest_affilate = dict()
                        try:
                            invest_affilate['name'] = netwo.css('a.network-row-investor-name ::text').get()
                        except:
                            invest_affilate['name'] = ''
                        try:
                            invest_affilate['image'] = netwo.css('img ::attr(src)').get()
                        except:
                            invest_affilate['image'] = ''
                        try:
                            invest_affilate['company'] = netwo.css('a.network-row-firm-name ::text').get()
                        except:
                            invest_affilate['company'] = ''
                        try:
                            invest_affilate['url'] = 'https://signal.nfx.com'+netwo.css('a.network-row-investor-name ::attr(href)').get()
                        except:
                            invest_affilate['url'] = ''

                        item['Scouts & Angels Affiliated'].append(invest_affilate)



        except:
            item['Invest With Investor'] = []
            item['Scouts & Angels Affiliated'] = []


        try:
            for connet in repsonse.css('div#vc-profile div.col-sm-3.col-xs-12 div.sn-margin-top-30'):
                if ('Networks' in ' '.join(connet.css('div.section-label ::text').extract())) and ('is a member of' in ' '.join(connet.css('div.section-label ::text').extract())):
                    item['Investor_Connections'] = []
                    for connet_connet in connet.css('div.mt2'):
                        connet_dict_dict = dict()
                        try:
                            connet_dict_dict['name'] = ' '.join(connet_connet.css('div.f6.sn-yellow-text ::text').extract())
                        except:
                            connet_dict_dict['name'] = ''
                        try:
                            connet_dict_dict['no of connections'] = ' '.join(connet_connet.css('div.f7.sans-serif.white-50 ::text').extract())
                        except:
                            connet_dict_dict['no of connections'] = ''
                        if len(connet_dict_dict) > 0:
                            item['Investor_Connections'].append(connet_dict_dict)

                if ('Find' in ' '.join(connet.css('p.section-label ::text').extract())) and ('on' in ' '.join(connet.css('p.section-label ::text').extract())):
                    item['Social Media'] = []
                    for connet_connet in connet.css('span.sn-linkset a ::attr(href)').extract():
                        connet_dict_dict = dict()

                        if 'linkedin' in connet_connet:
                            try:
                                connet_dict_dict['linkedin'] = connet_connet
                            except:
                                connet_dict_dict['linkedin'] = ''
                        if 'twitter' in connet_connet:
                            try:
                                connet_dict_dict['twitter'] = connet_connet
                            except:
                                connet_dict_dict['twitter'] = ''

                        if 'angel' in connet_connet:
                            try:
                                connet_dict_dict['angel'] = connet_connet
                            except:
                                connet_dict_dict['angel'] = ''

                        if 'crunchbase' in connet_connet:
                            try:
                                connet_dict_dict['crunchbase'] = connet_connet
                            except:
                                connet_dict_dict['crunchbase'] = ''

                        if len(connet_dict_dict) > 0:
                            item['Social Media'].append(connet_dict_dict)


        except:
            item['Investor_Connections'] = []
            item['Social Media'] = []



        for inter in repsonse.css('div.sn-margin-top-30.relative div.line-separated-row.row'):
            if inter.css('div.col-xs-5 span.section-label.lh-solid ::text').get() == 'Current Investing Position':
                try:
                    item['Current Investing Position'] = ' '.join(inter.css('div.col-xs-7 span.lh-solid ::text').extract())
                except:
                    item['Current Investing Position'] = ''

            if inter.css('div.col-xs-5 span.section-label.lh-solid ::text').get() == 'Investment Range':
                try:
                    item['Investment Range'] = ' '.join(inter.css('div.col-xs-7 span.lh-solid ::text').extract())
                except:
                    item['Investment Range'] = ''
            if inter.css('div.col-xs-5 span.section-label.lh-solid ::text').get() == 'Sweet Spot':
                try:
                    item['Sweet Spot'] = ' '.join(inter.css('div.col-xs-7 span.lh-solid ::text').extract())
                except:
                    item['Sweet Spot'] = ''

            if inter.css('div.col-xs-5 span.section-label.lh-solid ::text').get() == 'Investments On Record':
                try:
                    item['Investments On Record'] = ' '.join(inter.css('div.col-xs-7 span.lh-solid ::text').extract())
                except:
                    item['Investments On Record'] = ''

            if inter.css('div.col-xs-5 span.section-label.lh-solid ::text').get() == 'Current Fund Size':
                try:
                    item['Current Fund Size'] = ' '.join(inter.css('div.col-xs-7 span.lh-solid ::text').extract())
                except:
                    item['Current Fund Size'] = ''

        try:
            sectors = []
            for loop_type in repsonse.css('div.sn-margin-top-30.relative a.vc-list-chip'):
                sectors.append(' '.join(loop_type.css(' ::text').extract()))

            item['Sector & Stage'] = ','.join(sectors)
        except:
            item['Sector & Stage'] = ''


        peronId = repsonse.url.split('/')[-1]
        try:
            invets_recors = int([' '.join(v.css(' ::text').extract()) for v in repsonse.css('div.sn-margin-top-30.relative p.section-label') if 'Investments On Record' in ' '.join(v.css(' ::text').extract())][0].split('(')[-1][:-1])
        except:
            invets_recors = 0

        if invets_recors > 8:
            investor_graph_api = 'https://signal-api.nfx.com/graphql'
            investor_grap_pyload = {"operationName":"InvestorProfileSignedOutLoad","variables":{"personId":f'{peronId}',"firstInvestmentsOnRecord":invets_recors,"firstNetworkListInvestorProfiles":5,"firstInvestingConnections":3},"query":"query InvestorProfileSignedOutLoad($personId: ID!, $firstInvestmentsOnRecord: Int, $firstNetworkListInvestorProfiles: Int, $firstInvestingConnections: Int) {\n  investor_profile(person_id: $personId) {\n    ...signedOutInvestorProfileFields\n    __typename\n  }\n}\n\nfragment signedOutInvestorProfileFields on InvestorProfile {\n  id\n  person {\n    id\n    slug\n    first_name\n    last_name\n    name\n    linkedin_url\n    facebook_url\n    twitter_url\n    crunchbase_url\n    angellist_url\n    roles\n    url\n    first_degree_count\n    __typename\n  }\n  investor_profile_funding_rounds {\n    record_count\n    __typename\n  }\n  position\n  min_investment\n  max_investment\n  target_investment\n  image_urls\n  areas_of_interest_freeform\n  no_current_interest_freeform\n  vote_count\n  headline\n  previous_position\n  previous_firm\n  location {\n    id\n    display_name\n    __typename\n  }\n  firm {\n    id\n    current_fund_size\n    name\n    slug\n    __typename\n  }\n  degrees {\n    id\n    name\n    field_of_study\n    school {\n      id\n      name\n      display_name\n      total_student_count\n      __typename\n    }\n    __typename\n  }\n  positions {\n    id\n    title\n    company {\n      id\n      name\n      display_name\n      total_employee_count\n      __typename\n    }\n    start_date {\n      month\n      year\n      __typename\n    }\n    end_date {\n      month\n      year\n      __typename\n    }\n    __typename\n  }\n  media_links {\n    id\n    url\n    title\n    image_url\n    __typename\n  }\n  investor_lists {\n    id\n    slug\n    stage_name\n    vertical {\n      id\n      kind\n      display_name\n      __typename\n    }\n    __typename\n  }\n  investments_on_record(first: $firstInvestmentsOnRecord) {\n    pageInfo {\n      hasNextPage\n      __typename\n    }\n    record_count\n    edges {\n      node {\n        id\n        company_display_name\n        total_raised\n        coinvestor_names\n        investor_profile_funding_rounds {\n          id\n          is_lead\n          board_role {\n            id\n            title\n            __typename\n          }\n          funding_round {\n            id\n            stage\n            date\n            amount\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  network_list_investor_profiles(first: $firstNetworkListInvestorProfiles) {\n    list_type\n    edges {\n      node {\n        id\n        image_urls\n        position\n        person {\n          id\n          name\n          first_name\n          last_name\n          slug\n          __typename\n        }\n        firm {\n          id\n          name\n          slug\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  network_list_scouts_and_angels_profiles(first: $firstNetworkListInvestorProfiles) {\n    list_type\n    edges {\n      node {\n        id\n        image_urls\n        position\n        person {\n          id\n          name\n          first_name\n          last_name\n          slug\n          __typename\n        }\n        firm {\n          id\n          name\n          slug\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  investing_connections(first: $firstInvestingConnections) {\n    record_count\n    edges {\n      node {\n        id\n        target_person {\n          id\n          name\n          slug\n          first_name\n          last_name\n          investor_profile {\n            id\n            image_urls\n            firm {\n              id\n              name\n              slug\n              __typename\n            }\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n"}

            headers_investor = {
                "authority": "signal-api.nfx.com",
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9,de;q=0.8",
                "content-type": "application/json",
                "origin": "https://signal.nfx.com",
                "referer": "https://signal.nfx.com/",
                "sec-ch-ua": "\"Google Chrome\";v=\"105\", \"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"105\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-site",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"
            }
            item['Past Investment Records'] = []

            response_investor_api = json.loads(requests.post(url=investor_graph_api,headers=headers_investor,data=json.dumps(investor_grap_pyload)).text)

            for koop in response_investor_api['data']['investor_profile']['investments_on_record']['edges']:
                invest_list = dict()
                invest_list['company'] = koop['node']['company_display_name']
                try:
                    invest_list['total_raised'] = koop['node']['total_raised'][0]
                except:
                    invest_list['total_raised'] = ''



                invest_list['coinvestor_names'] = ','.join(koop['node']['coinvestor_names'])
                invest_list['funding_rounds'] = []


                # invest_list['coinvestor_names'] = ','.join(koop['node']['coinvestor_names'])
                for inner_koop in koop['node']['investor_profile_funding_rounds']:
                    funding_rou = dict()
                    funding_rou['stage'] = inner_koop['funding_round']['stage']
                    funding_rou['date'] = inner_koop['funding_round']['date']
                    funding_rou['amount'] = inner_koop['funding_round']['amount']
                    invest_list['funding_rounds'].append(funding_rou)

                item['Past Investment Records'].append(invest_list)
        else:
            # [v for v in repsonse.css('script ::text').extract() if 'window.__PRELOADED_STATE__' in v ][0].split('window.__APOLLO_STATE__ = ')[-1]
            item['Past Investment Records'] = []
            for table_loop in repsonse.css('tbody.past-investments-table-body tr'):
                invest_list2 = dict()
                for index_count ,inner_table_loop in enumerate(table_loop.css('td')):
                    if index_count == 0:
                        invest_list2['company'] = inner_table_loop.css('div ::text').get()
                    if index_count == 1:
                        invest_list2['funding_rounds'] = []
                        for inner_inner_loop in inner_table_loop.css('div.round-padding'):
                            funding_rouns = dict()
                            try:
                                funding_rouns['stage'] = inner_inner_loop.css(' ::text').extract()[0]
                            except:
                                a=1
                                pass
                            try:
                                funding_rouns['date'] = inner_inner_loop.css(' ::text').extract()[1]
                            except:
                                a=1
                                pass
                            try:
                                funding_rouns['amount'] = inner_inner_loop.css(' ::text').extract()[2]
                            except:
                                a=1
                                pass
                            try:
                                invest_list2['funding_rounds'].append(funding_rouns)
                            except:
                                a=1
                                pass

                    if index_count == 2:
                        invest_list2['total_raised'] = inner_table_loop.css('div ::text').get()
                if invest_list2['company']!=None:
                    item['Past Investment Records'].append(invest_list2)


        for experiern_loop in repsonse.css('div.sn-margin-top-30.relative'):
            try:
                if 'Experience' in ' '.join(experiern_loop.css('p.section-label ::text').extract()):
                    item['Experience'] = []
                    for inner_exp in experiern_loop.css('div.line-separated-row.flex.justify-between'):
                        item['Experience'].append(' '.join(inner_exp.css('span ::text').extract()))
            except:
                pass

            try:
                if 'Education' in ' '.join(experiern_loop.css('p.section-label ::text').extract()):
                    item['Education'] = []
                    for inner_exp in experiern_loop.css('div.line-separated-row'):
                        item['Education'].append(' '.join(inner_exp.css(' ::text').extract()))
            except:
                pass

        """ User Profile Insert Operation"""

        try:
            profile_name = item['name']
        except:
            profile_name = ''

        try:
            profile_url = item['url']
        except:
            profile_url =''


        try:
            profile_image = item['image']
        except:
            profile_image = ''

        try:
            profile_type = item['Type']
        except:
            profile_type = ''

        try:
            profile_firm = item['firm']
        except:
            profile_firm = ''
        try:
            profile_location = item['location']
        except:
            profile_location = ''
        try:
            profile_website = item['website']
        except:
            profile_website = ''

        try:
            profile_investing_position = item['Current Investing Position']
        except:
            profile_investing_position = ''

        try:
            profile_investment_range = item['Investment Range']
        except:
            profile_investment_range = ''

        try:
            profile_SweetSpot= item['Sweet Spot']
        except:
            profile_SweetSpot = ''

        try:
            profile_invest_on_record= item['Investments On Record']
        except:
            profile_invest_on_record = ''

        try:
            profile_current_fund_size= item['Current Fund Size']
        except:
            profile_current_fund_size = ''

        try:
            profile_sector_stage= item['Sector & Stage']
        except:
            profile_sector_stage = ''

        try:
            profile_experiece= ','.join(item['Experience'])
        except:
            profile_experiece = ''

        try:
            profile_education= ','.join(item['Education'])
        except:
            profile_education = ''

        profile_linkedin = ''
        profile_twitter = ''
        profile_angel = ''
        profile_crunchbase = ''
        try:
            for social_loop in item['Social Media']:

                try:
                    profile_linkedin = social_loop['linkedin']
                except:
                    pass
                try:
                    profile_twitter = social_loop['twitter']
                except:
                    pass
                try:
                    profile_angel = social_loop['angel']
                except:
                    pass

                try:
                    profile_crunchbase = social_loop['crunchbase']
                except:
                    pass


        except:
            profile_linkedin = ''
            profile_twitter = ''
            profile_angel = ''
            profile_crunchbase = ''

        sql_read = f'SELECT * FROM profile where profile.profile_url="{profile_url}";'
        mycursor.execute(sql_read)
        desc = mycursor.description
        column_names = [col[0] for col in desc]
        existed_product_database = [dict(zip(column_names, row)) for row in mycursor.fetchall()]

        if len(existed_product_database) > 0:
            get_id_profile_for_updation = existed_product_database[0]['id']
            if (existed_product_database[0]['name'] != profile_name) or (existed_product_database[0]['profile_url'] != profile_url) or (existed_product_database[0]['image'] != profile_image)or (existed_product_database[0]['type'] != profile_type)or (existed_product_database[0]['firm'] != profile_firm) or (existed_product_database[0]['location'] != profile_location) or (existed_product_database[0]['website'] != profile_website) or (existed_product_database[0]['current_investing_position'] != profile_investing_position) or (existed_product_database[0]['investment_range'] != profile_investment_range) or (existed_product_database[0]['sweetspot'] != profile_SweetSpot) or (existed_product_database[0]['investment_on_record'] != profile_invest_on_record) or (existed_product_database[0]['current_fund_size'] != profile_current_fund_size) or (existed_product_database[0]['sector_stage'] != profile_sector_stage) or (existed_product_database[0]['experience'] != profile_experiece) or (existed_product_database[0]['education'] != profile_education) or (existed_product_database[0]['linkedin'] != profile_linkedin) or (existed_product_database[0]['twitter'] != profile_twitter) or (existed_product_database[0]['angel'] != profile_angel) or (existed_product_database[0]['crunchbase'] != profile_crunchbase):
                """ Update Profile Table Data """
                sql_update_query = f"UPDATE profile set name='{profile_name}',profile_url='{profile_url}', image='{profile_image}', type='{profile_type}', firm='{profile_firm}', location='{profile_location}',website='{profile_website}',current_investing_position='{profile_investing_position}',investment_range='{profile_investment_range}',sweetspot='{profile_SweetSpot}',investment_on_record='{profile_invest_on_record}',current_fund_size='{profile_current_fund_size}',sector_stage='{profile_sector_stage}',experience='{profile_experiece}',education='{profile_education}',linkedin='{profile_linkedin}',twitter='{profile_twitter}',angel='{profile_angel}',crunchbase='{profile_crunchbase}' where profile_url='{profile_url}'"

                mycursor.execute(sql_update_query)
                mydb.commit()
                print('Profile Record Updated Succesfully!!')

            """Check Invest With Investor Table  """
            sql_read_investor = f'SELECT * FROM invest_with_investor where invest_with_investor.profile_id="{get_id_profile_for_updation}";'
            mycursor.execute(sql_read_investor)
            desc = mycursor.description
            column_names = [col[0] for col in desc]
            get_investor_data = [dict(zip(column_names, row)) for row in mycursor.fetchall()]
            if len(get_investor_data) > 0:
                try:
                    for new_investor_with in item['Invest With Investor']:
                        try:
                            name_investor_check_update = new_investor_with['name']
                        except:
                            name_investor_check_update = ''

                        try:
                            image_investor_check_update = new_investor_with['image']
                        except:
                            image_investor_check_update = ''
                        try:
                            company_investor_check_update = new_investor_with['company']
                        except:
                            company_investor_check_update = ''

                        try:
                            url_investor_check_update = new_investor_with['url']
                        except:
                            url_investor_check_update = ''

                        sql_read_update_investor = f'SELECT * FROM invest_with_investor where invest_with_investor.url="{url_investor_check_update}";'
                        mycursor.execute(sql_read_update_investor)
                        desc = mycursor.description
                        column_names = [col[0] for col in desc]
                        data_investor_db = [dict(zip(column_names, row)) for row in mycursor.fetchall()]

                        if len(data_investor_db) > 0:
                            if (data_investor_db[0]['name'] != name_investor_check_update) or (data_investor_db[0]['image'] != image_investor_check_update) or (data_investor_db[0]['company'] != company_investor_check_update) or (data_investor_db[0]['url'] != url_investor_check_update):
                                """ Update Invest with investor  Table Data """
                                sql_update_query = f"UPDATE invest_with_investor set name='{name_investor_check_update}',image='{image_investor_check_update}', company='{company_investor_check_update}',  where url = '{url_investor_check_update}'"
                                mycursor.execute(sql_update_query)
                                mydb.commit()
                                print('Profile Record Updated Succesfully!!')

                except:
                    pass
            else:
                try:
                    if len(item['Invest With Investor']) > 0:
                        for investor_forloop_else in item['Invest With Investor']:
                            try:
                                name_investor_check_else_update = investor_forloop_else['name']
                            except:
                                name_investor_check_else_update = ''

                            try:
                                image_investor_check_else_update = investor_forloop_else['image']
                            except:
                                image_investor_check_else_update = ''
                            try:
                                company_investor_check_else_update = investor_forloop_else['company']
                            except:
                                company_investor_check_else_update = ''

                            try:
                                url_investor_check_else_update = investor_forloop_else['url']
                            except:
                                url_investor_check_else_update = ''

                            sql = "INSERT INTO invest_with_investor (profile_id,name,image,company,url) VALUES (%s,%s,%s,%s,%s)"
                            val = (get_id_profile_for_updation, name_investor_check_else_update, image_investor_check_else_update, company_investor_check_else_update,url_investor_check_else_update)

                            mycursor.execute(sql, val)
                            mydb.commit()
                except:
                    pass

            """ Check Scout Angel Affiliated Table  """
            sql_read_update_Scout = f'SELECT * FROM scout_angel_affiliated where scout_angel_affiliated.profile_id="{get_id_profile_for_updation}";'
            mycursor.execute(sql_read_update_Scout)
            desc = mycursor.description
            column_names = [col[0] for col in desc]
            get_scout_data_db = [dict(zip(column_names, row)) for row in mycursor.fetchall()]
            if len(get_scout_data_db) > 0:
                try:
                    for new_scout_with in item['Scouts & Angels Affiliated']:
                        try:
                            name_scout_check_update = new_scout_with['name']
                        except:
                            name_scout_check_update = ''

                        try:
                            image_scout_check_update = new_scout_with['image']
                        except:
                            image_scout_check_update = ''
                        try:
                            company_scout_check_update = new_scout_with['company']
                        except:
                            company_scout_check_update = ''

                        try:
                            url_scout_check_update = new_scout_with['url']
                        except:
                            url_scout_check_update = ''

                        sql_read_update_scout = f'SELECT * FROM scout_angel_affiliated where scout_angel_affiliated.url="{url_scout_check_update}";'
                        mycursor.execute(sql_read_update_scout)
                        desc = mycursor.description
                        column_names = [col[0] for col in desc]
                        data_investor_db = [dict(zip(column_names, row)) for row in mycursor.fetchall()]

                        if len(data_investor_db) > 0:
                            if (data_investor_db[0]['name'] != name_scout_check_update) or (
                                    data_investor_db[0]['image'] != image_scout_check_update) or (
                                    data_investor_db[0]['company'] != company_scout_check_update) or (
                                    data_investor_db[0]['url'] != url_scout_check_update):
                                """ Update Invest with investor  Table Data """
                                sql_update_query = f"UPDATE scout_angel_affiliated set name='{name_scout_check_update}',image='{image_scout_check_update}', company='{company_scout_check_update}',  where url = '{url_scout_check_update}'"
                                mycursor.execute(sql_update_query)
                                mydb.commit()
                                print('Scout Record Updated Succesfully!!')

                except:
                    pass
            else:
                try:
                    if len(item['Scouts & Angels Affiliated']) > 0:
                        for scout_forloop_else in item['Scouts & Angels Affiliated']:
                            try:
                                name_scout_check_else_update = scout_forloop_else['name']
                            except:
                                name_scout_check_else_update = ''

                            try:
                                image_scout_check_else_update = scout_forloop_else['image']
                            except:
                                image_scout_check_else_update = ''
                            try:
                                company_scout_check_else_update = scout_forloop_else['company']
                            except:
                                company_scout_check_else_update = ''

                            try:
                                url_scout_check_else_update = scout_forloop_else['url']
                            except:
                                url_scout_check_else_update = ''

                            sql = "INSERT INTO scout_angel_affiliated (profile_id,name,image,company,url) VALUES (%s,%s,%s,%s,%s)"
                            val = (get_id_profile_for_updation, name_scout_check_else_update, image_scout_check_else_update, company_scout_check_else_update,url_scout_check_else_update)

                            mycursor.execute(sql, val)
                            mydb.commit()
                except:
                    pass

            """ Check Investor Connection Table  """
            sql_read_update_connection = f'SELECT * FROM investor_connecions where investor_connecions.profile_id="{get_id_profile_for_updation}";'
            mycursor.execute(sql_read_update_connection)
            desc = mycursor.description
            column_names = [col[0] for col in desc]
            get_connection_data_db = [dict(zip(column_names, row)) for row in mycursor.fetchall()]
            if len(get_connection_data_db) > 0:
                try:
                    for new_connection_with in item['Investor_Connections']:
                        connection_existed = True
                        for old_connections in get_connection_data_db:
                            if (new_connection_with['name'] == old_connections['name']) and (new_connection_with['no of connections'] == old_connections['no_of_connections']):
                                connection_existed = False
                                break
                        if connection_existed:
                            sql = "INSERT INTO investor_connecions (profile_id,name,no_of_connections) VALUES (%s,%s,%s)"
                            val = (get_id_profile_for_updation, new_connection_with['name'],new_connection_with['no of connections'])

                            mycursor.execute(sql, val)
                            mydb.commit()

                except:
                    pass
            else:
                try:
                    if len(item['Investor_Connections']) > 0:
                        for connection_forloop_else in item['Investor_Connections']:
                            try:
                                name_connection_check_else_update = connection_forloop_else['name']
                            except:
                                name_connection_check_else_update = ''

                            try:
                                no_connection_check_else_update = connection_forloop_else['no_of_connections']
                            except:
                                no_connection_check_else_update = ''


                            sql = "INSERT INTO investor_connecions (profile_id,name,no_of_connections) VALUES (%s,%s,%s)"
                            val = (get_id_profile_for_updation, name_connection_check_else_update, no_connection_check_else_update)

                            mycursor.execute(sql, val)
                            mydb.commit()
                except:
                    pass

            """ Check Past Invest Records Table  """

            sql_read_update_past_investment = f'SELECT * FROM past_investment_records where past_investment_records.profile_id="{get_id_profile_for_updation}";'
            mycursor.execute(sql_read_update_past_investment)
            desc = mycursor.description
            column_names = [col[0] for col in desc]
            get_past_investment_data_db = [dict(zip(column_names, row)) for row in mycursor.fetchall()]

            if len(get_past_investment_data_db) > 0:
                try:
                    for new_loop_past_investments in item['Past Investment Records']:
                        past_investment_existed = True
                        for old_past_investments in get_past_investment_data_db:
                            try:
                                just_ckeck = new_loop_past_investments['coinvestor_names']
                            except:
                                new_loop_past_investments['coinvestor_names'] = ''
                            try:
                                just_ckeck = new_loop_past_investments['total_raised']
                            except:
                                new_loop_past_investments['total_raised'] = ''

                            if (new_loop_past_investments['company'] == old_past_investments['company']) and (new_loop_past_investments['total_raised'] == old_past_investments['total_raised']) and (new_loop_past_investments['coinvestor_names'] == old_past_investments['co_investors']):
                                past_investment_existed = False
                                inner_loop_update_funding = old_past_investments['funding_id']

                                sql_read_update_funding_round = f'SELECT * FROM funding_rounds where funding_rounds.funding_id="{inner_loop_update_funding}";'
                                mycursor.execute(sql_read_update_funding_round)
                                desc = mycursor.description
                                column_names = [col[0] for col in desc]
                                get_funding_round_data_db_update = [dict(zip(column_names, row)) for row in mycursor.fetchall()]

                                for new_loop_funding_round in new_loop_past_investments['funding_rounds']:
                                    funding_round_data_existed= True
                                    for old_funding_round in get_funding_round_data_db_update:
                                        try:
                                            check_just = new_loop_funding_round['date']
                                        except:
                                            new_loop_funding_round['date'] = old_funding_round['date']
                                        try:
                                            check_just = new_loop_funding_round['amount']
                                        except:
                                            new_loop_funding_round['amount'] = old_funding_round['amount']


                                        if (new_loop_funding_round['stage'] == old_funding_round['stage']) and (new_loop_funding_round['date']  == old_funding_round['date']) and (new_loop_funding_round['amount'] == old_funding_round['amount']):
                                            funding_round_data_existed = False
                                            break
                                    if funding_round_data_existed:
                                        sql = "INSERT INTO funding_rounds (funding_id,stage,date,amount) VALUES (%s,%s,%s,%s)"
                                        val = (inner_loop_update_funding,new_loop_funding_round['stage'] ,new_loop_funding_round['date']  , new_loop_funding_round['amount'])
                                        mycursor.execute(sql, val)
                                        mydb.commit()
                                break
                        if past_investment_existed:

                            try:
                                temp = new_loop_past_investments['total_raised']
                            except:
                                new_loop_past_investments['total_raised'] = old_past_investments['total_raised']

                            try:
                                temp = new_loop_past_investments['coinvestor_names']
                            except:
                                new_loop_past_investments['coinvestor_names'] = old_past_investments['co_investors']

                            try:
                                temp = new_loop_past_investments['company']
                            except:
                                new_loop_past_investments['company'] = old_past_investments['company']

                            sql = "INSERT INTO past_investment_records (profile_id,company,total_raised,co_investors) VALUES (%s,%s,%s,%s)"
                            val = (get_id_profile_for_updation, new_loop_past_investments['company'], new_loop_past_investments['total_raised'], new_loop_past_investments['coinvestor_names'])


                            mycursor.execute(sql, val)
                            mydb.commit()

                            try:
                                if len(new_loop_past_investments['funding_rounds']) > 0:
                                    funding_sql_read_updated = f'SELECT * FROM past_investment_records where profile_id={get_id_profile_for_updation};'
                                    mycursor.execute(funding_sql_read_updated)
                                    funding_desc = mycursor.description
                                    column_names = [col[0] for col in funding_desc]
                                    existed_funding_table_updated = [dict(zip(column_names, row)) for row in
                                                             mycursor.fetchall()]
                                    get_funding_id_id = max([g['funding_id'] for g in existed_funding_table_updated])

                                    for funding_loop_funding in new_loop_past_investments['funding_rounds']:
                                        try:
                                            fund_stage_round = funding_loop_funding['stage']

                                        except:
                                            fund_stage_round = ''

                                        try:
                                            fund_date_round = funding_loop_funding['date']

                                        except:
                                            fund_date_round = ''

                                        try:
                                            fund_amount_round = funding_loop_funding['amount']

                                        except:
                                            fund_amount_round = ''

                                        sql = "INSERT INTO funding_rounds (funding_id,stage,date,amount) VALUES (%s,%s,%s,%s)"
                                        val = (get_funding_id_id, fund_stage_round, fund_date_round, fund_amount_round)

                                        mycursor.execute(sql, val)
                                        mydb.commit()


                            except:
                                pass


                except:
                    pass

            else:
                try:
                    if len(item['Past Investment Records']) > 0:
                        for past_investment_forloop_else in item['Past Investment Records']:
                            try:
                                company_past_investment_check_else_update = past_investment_forloop_else['company']
                            except:
                                company_past_investment_check_else_update = ''

                            try:
                                total_raise_past_investment_check_else_update = past_investment_forloop_else['total_raised']
                            except:
                                total_raise_past_investment_check_else_update = ''

                            try:
                                coinvestor_past_investment_check_else_update = past_investment_forloop_else['coinvestor_names']
                            except:
                                coinvestor_past_investment_check_else_update = ''



                            sql = "INSERT INTO past_investment_records (profile_id,company,total_raised,co_investors) VALUES (%s,%s,%s,%s)"
                            val = (get_id_profile_for_updation, company_past_investment_check_else_update, total_raise_past_investment_check_else_update,coinvestor_past_investment_check_else_update)

                            mycursor.execute(sql, val)
                            mydb.commit()

                            if len(past_investment_forloop_else['funding_rounds']) > 0:
                                funding_sql_read_update = f'SELECT * FROM past_investment_records where profile_id={get_id_profile_for_updation};'
                                mycursor.execute(funding_sql_read_update)
                                funding_desc = mycursor.description
                                column_names = [col[0] for col in funding_desc]
                                existed_funding_table = [dict(zip(column_names, row)) for row in mycursor.fetchall()]
                                get_funding_id_update = max([g['funding_id'] for g in existed_funding_table])

                                for funding_loop_new in past_investment_forloop_else['funding_rounds']:
                                    try:
                                        fund_stage_update = funding_loop_new['stage']
                                    except:
                                        fund_stage_update  = ''

                                    try:
                                        fund_date_update = funding_loop_new['date']

                                    except:
                                        fund_date_update  = ''

                                    try:
                                        fund_amount_update = funding_loop_new['amount']
                                        # if fund_amount == None:
                                        #     fund_amount = ''
                                    except:
                                        fund_amount_update  = ''


                                    sql = "INSERT INTO funding_rounds (funding_id,stage,date,amount) VALUES (%s,%s,%s,%s)"
                                    val = (get_funding_id_update,fund_stage_update,fund_date_update,fund_amount_update)

                                    mycursor.execute(sql, val)
                                    mydb.commit()

                except:
                    pass




        else:
            sql = "INSERT INTO profile (name,profile_url, image, type, firm, location,website,current_investing_position,investment_range,sweetspot,investment_on_record,current_fund_size,sector_stage,experience,education,linkedin,twitter,angel,crunchbase) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            val = (profile_name, profile_url ,profile_image, profile_type, profile_firm, profile_location,profile_website,profile_investing_position,profile_investment_range,profile_SweetSpot,profile_invest_on_record,profile_current_fund_size,profile_sector_stage,profile_experiece,profile_education,profile_linkedin,profile_twitter,profile_angel,profile_crunchbase)

            mycursor.execute(sql, val)
            mydb.commit()

            """ get id from the profile table """

            sql_read = f'SELECT * FROM profile where profile.profile_url="{profile_url}";'
            mycursor.execute(sql_read)
            desc = mycursor.description
            column_names = [col[0] for col in desc]
            get_profile_id = [dict(zip(column_names, row)) for row in mycursor.fetchall()]
            item_profile_id = get_profile_id[0]['id']

            try:
                """Insert Data in invest_with_investors Table"""
                if len(item['Invest With Investor']) > 0:
                    for investor_forloop in item['Invest With Investor']:
                        try:
                            name_investor_check  = investor_forloop['name']
                        except:
                            name_investor_check = ''

                        try:
                            image_investor_check  = investor_forloop['image']
                        except:
                            image_investor_check = ''
                        try:
                            company_investor_check = investor_forloop['company']
                        except:
                            company_investor_check = ''
                        try:
                            url_investor_check = investor_forloop['url']
                        except:
                            url_investor_check = ''

                        # if name_investor_check == None:
                        #     name_investor_check  = ''
                        #
                        # if image_investor_check == None:
                        #     image_investor_check  = ''
                        #
                        # if company_investor_check == None:
                        #     company_investor_check = ''



                        sql = "INSERT INTO invest_with_investor (profile_id,name,image,company,url) VALUES (%s,%s,%s,%s,%s)"
                        val = (item_profile_id,name_investor_check,image_investor_check,company_investor_check,url_investor_check)

                        mycursor.execute(sql, val)
                        mydb.commit()




            except:
                pass



            try:
                """Insert Data in Scouts & Angels Table"""
                if len(item['Scouts & Angels Affiliated']) > 0:
                    for Scouts_forloop in item['Scouts & Angels Affiliated']:
                        try:
                            name_scout_check  = Scouts_forloop['name']
                        except:
                            name_scout_check = ''
                        try:
                            image_scout_check  = Scouts_forloop['image']
                        except:
                            image_scout_check = ''
                        try:
                            company_scout_check = Scouts_forloop['company']
                        except:
                            company_scout_check = ''

                        try:
                            url_scout_check = Scouts_forloop['url']
                        except:
                            url_scout_check = ''

                        sql = "INSERT INTO scout_angel_affiliated (profile_id,name,image,company,url) VALUES (%s,%s,%s,%s,%s)"
                        val = (item_profile_id,name_scout_check,image_scout_check,company_scout_check,url_scout_check)

                        mycursor.execute(sql, val)
                        mydb.commit()


            except:
                pass


            try:
                """Insert Data in investor connection Table"""
                if len(item['Investor_Connections']) > 0:
                    for connection_forloop in item['Investor_Connections']:
                        try:
                            name_connection_check  = connection_forloop['name']
                        except:
                            name_connection_check = ''
                        try:
                            no_connection_check  = connection_forloop['no of connections']
                        except:
                            no_connection_check = ''


                        sql = "INSERT INTO investor_connecions (profile_id,name,no_of_connections) VALUES (%s,%s,%s)"
                        val = (item_profile_id,name_connection_check,no_connection_check)

                        mycursor.execute(sql, val)
                        mydb.commit()


            except:
                pass

            try:
                """Insert Data in past_investment_records Table"""
                if len(item['Past Investment Records']) > 0:
                    for past_forloop in item['Past Investment Records']:
                        try:
                            company_past_check  = past_forloop['company']



                        except:
                            company_past_check = ''
                        try:
                            total_raised_past_check  = past_forloop['total_raised']

                        except:
                            total_raised_past_check = ''

                        try:
                            coinvestor_names_past_check  = past_forloop['coinvestor_names']

                        except:
                            coinvestor_names_past_check = ''



                        sql = "INSERT INTO past_investment_records (profile_id,company,total_raised,co_investors) VALUES (%s,%s,%s,%s)"
                        val = (item_profile_id,company_past_check,total_raised_past_check,coinvestor_names_past_check)

                        mycursor.execute(sql, val)
                        mydb.commit()

                        try:
                            if len(past_forloop['funding_rounds']) > 0:
                                funding_sql_read = f'SELECT * FROM past_investment_records where profile_id={item_profile_id};'
                                mycursor.execute(funding_sql_read)
                                funding_desc = mycursor.description
                                column_names = [col[0] for col in funding_desc]
                                existed_funding_table = [dict(zip(column_names, row)) for row in mycursor.fetchall()]
                                get_funding_id = max([g['funding_id'] for g in existed_funding_table ])

                                for funding_loop in past_forloop['funding_rounds']:
                                    try:
                                        fund_stage = funding_loop['stage']
                                        # if fund_stage == None:
                                        #     fund_stage = ''
                                    except:
                                        fund_stage  = ''

                                    try:
                                        fund_date = funding_loop['date']

                                    except:
                                        fund_date  = ''

                                    try:
                                        fund_amount = funding_loop['amount']
                                        # if fund_amount == None:
                                        #     fund_amount = ''
                                    except:
                                        fund_amount  = ''




                                    sql = "INSERT INTO funding_rounds (funding_id,stage,date,amount) VALUES (%s,%s,%s,%s)"
                                    val = (get_funding_id,fund_stage,fund_date,fund_amount)

                                    mycursor.execute(sql, val)
                                    mydb.commit()
                                    a=1


                        except:
                            pass


            except:
                pass
        a=1
        # main_list.append(item)


process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

process.crawl(investor)
process.start()
