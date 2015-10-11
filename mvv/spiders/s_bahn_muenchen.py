# -*- coding: utf-8 -*-
import scrapy
from datetime import datetime
from mvv.items import MvvItem
import json
import keen
from keen.client import KeenClient
from scrapy.utils.project import data_path

class SBahnMuenchenSpider(scrapy.Spider):
    # keenio settings
    keen.project_id = "561925d196773d74b138cefd"
    keen.write_key = "778398ca03da00a90bd9b25f064f2701015cb1bb117e76a64d16b886c6080f34031e737000fa539994b1b7adeb12e3ad714240ae4d321212adf4b59b9bfaf748d375f91344179ddb70df5e5ef76dc23b91643f926fa7abdeb4b7fbf810f26a0994f0774c499bac69644c2aa9de4cd480"
    keen.read_key = "XXX"

    name = "s-bahn-muenchen"
    allowed_domains = ["s-bahn-muenchen.de"]
    start_urls = (
        'http://img.srv2.de/customer/sbahnMuenchen/newsticker/newsticker.html',
        #'file://127.0.0.1/mnt/data/Repositories/mvv/example/response3.html',
    )

    def parse(self, response):
        persistence_path = data_path(".scrapy")
        persistence_load = []
        notifications = []
        try:
            persistence_load = json.load(open(persistence_path+"_persistence.json"))
        except Exception: 
            pass

        def parse_notification(notification):
            mvv_item = MvvItem()

            # line
            notification_line = notification.xpath('.//div[@class="leftColumn"]/img/@title').extract()
            mvv_item['line'] = notification_line

            # last update time
            dateElement = notification.xpath('.//span[@class="lastUpdateTime"]/text()').extract()[0].strip()
            dateElementSplit = dateElement.split(": ")[1].strip()
            lastUpdateTime = datetime.strptime(dateElementSplit, '%Y-%m-%d %H:%M:%S')
            lastUpdateTime = lastUpdateTime.strftime("%Y-%m-%d %H:%M:%S")
            mvv_item['lastUpdateTime'] = lastUpdateTime

            # report long
            reportLong = notification.xpath('.//div[@class="rightColumn"]/h1/text()').extract()[0].strip()
            mvv_item['reportLong'] = reportLong

            # report short
            # extract all divs
            allDivs = notification.xpath('.//div[@class="rightColumn"]')[0].xpath('div').extract()
            # get content of last div
            lastDiv = notification.xpath('.//div[@class="rightColumn"]')[0].xpath('div')[len(allDivs)-1]
            reportShort = lastDiv.xpath('p/text()').extract()[0].strip()
            mvv_item['reportShort'] = reportShort
            
            # tracks
            tracks = notification.xpath('.//div[@class="rightColumn"]/div[@class="tracks"]')[0].xpath('text()')[1].extract().strip()
            mvv_item['tracks'] = tracks

            keen.add_event("log", {
                'line': line,
                'lastUpdateTime': lastUpdateTime,
                'reportLong': reportLong,
                'reportShort': reportShort,
                'tracks': tracks
        	})

            notifications.append(mvv_item)

        def json_serial(obj):
            if isinstance(obj, set):
                return list(obj)
            return obj.__dict__

        def change_detection(notifications, persistence_load):
            combination = list_combination(notifications, persistence_load)

            for idx_line, line in enumerate(combination):
                hit_notifications = False
                hit_persistence = False
                notification = ""
                for idx_persistence, val_persistence in enumerate(persistence_load):
                    if(line == persistence_load[idx_persistence]['_values']['line']):
                    	hit_persistence = True
                    	notification = val_persistence
                for idx_notifications, val_notifications in enumerate(notifications):
                    if(line == notifications[idx_notifications]['line']):
                        hit_notifications = True
                        notification = val_notifications

                print('--------------------------------')
                if(hit_persistence and hit_notifications):
                    print("nothing changed: value found in website and in persistence: "+ line[0])
                else:
                    if(hit_persistence):
                        print("change detected: situation resolved: "+ line[0])
                        keenio_send_resolved(notification)
                    if(hit_notifications):
                        print("change detected: new situation: "+ line[0])
                        keenio_send_situation(notification)
                print('--------------------------------')

        def keenio_send_situation(notification):
            line = notification['line'][0]
            lineReplace = line.replace(" ", "")
            event = {
                'line': line,
                'lastUpdateTime': notification['lastUpdateTime'],
                'reportLong': notification['reportLong'],
                'reportShort': notification['reportShort'],
                'tracks': notification['tracks']
            }
            keen.add_event("notifications_start", event)
            json.dump(event, open(persistence_path+"_persistence_"+lineReplace+".json",'w'), default=json_serial, indent=4)

        def keenio_send_resolved(notification):
            #notification = notification['_values']
            line = notification['_values']['line'][0]
            lineReplace = line.replace(" ", "")
            path = persistence_path+"_persistence_"+lineReplace+".json"
            persistence = {}
            try:
                persistence = json.load(open(path))
            except Exception: 
                pass
            event = {
                'line': line,
                'startTime': notification['_values']['lastUpdateTime'],
                'endTime': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'reportLong': notification['_values']['reportLong'],
                'reportShort': notification['_values']['reportShort'],
                'tracks': notification['_values']['tracks']
            }
            keen.add_event(lineReplace, event)
            json.dump({}, open(path,'w'), default=json_serial, indent=4)

        # combination of the two lists with extracted lines
        def list_combination(notifications, persistence_load):
            resulting_list = []
            for idx_persistence, val_persistence in enumerate(persistence_load):
            	line = persistence_load[idx_persistence]['_values']['line']
            	if line not in resulting_list:
                   resulting_list.append(line)

            for idx_notifications, val_notifications in enumerate(notifications):
            	line = notifications[idx_notifications]['line']
            	if line not in resulting_list:
                   resulting_list.append(line)

            return resulting_list

        for notification in response.xpath('//div[@class="notification"]'):
            parse_notification(notification)

        json.dump(notifications, open(persistence_path+"_persistence.json",'w'), default=json_serial, indent=4)
        change_detection(notifications, persistence_load)

        return notifications
