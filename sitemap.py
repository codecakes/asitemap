import os
import urlparse
import errno
import time
import datetime
import gzip
import pytz
from StringIO import StringIO
from flask import url_for, Flask
from flask.ext.mongoengine import MongoEngine
from flask.ext.script import Manager, Command, Option
import jinja2


app = Flask(__name__)

app.config['MONGODB_DB'] = 'sitemap'
#app.config['MONGODB_DB'] = os.path.abspath('sitemap')
db = MongoEngine(app)

manager = Manager(app)


class User(db.Document):
    uid = db.SequenceField()
    username = db.StringField()
    last_accessed = db.DateTimeField(default=datetime.datetime.utcnow)

    #Added for direct access of 'artwork_aid's and 'product_detail_pid's
    #via a file
    artwork_meta_aid_file = db.StringField()
    product_meta_pid_file = db.StringField()


class Artwork(db.Document):
    aid = db.SequenceField()
    uid = db.IntField()
    slug = db.StringField()
    last_modified = db.DateTimeField(default=datetime.datetime.utcnow)


class ProductDetail(db.Document):
    pid = db.SequenceField()
    aid = db.IntField()
    pname = db.StringField()
    last_modified = db.DateTimeField(default=datetime.datetime.utcnow)


class GenerateSitemap(Command):
    """
    Generates sitemap for the given app
    """

    option_list = [
        Option('--verbose', '-v', dest='verbose', action='store_true',
               default=False),
        Option('--dry', '-d', dest='dry', action='store_true',
               default=False),
    ]

    sitemap_loc = 'templates'
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(sitemap_loc),
                             autoescape=True)

    def setup_defaults_from_config(self):
        """
        Sets up the default limits from the current_app config.
        """
        # max number of urls per sitemap
        self.max_urls_per_page = 50000
        # max size of a sitemap in bytes
        self.max_sitemap_size = 10000000
        # sitemap output directory
        self.sitemap_out = '/tmp/sitemap/'
        # base url
        self.base_url = 'example.com'

    def get_size(self, obj):
        """
        Get filesize of object on disk. used to check 10 mb limit
        """

        template = self.env.get_template('sitemap.xml')
        output_from_parsed_template = template.render(pages=obj)

        s = StringIO()
        s.write(output_from_parsed_template)
        s.seek(0, os.SEEK_END)
        return s.tell()

    def add_timezone(self, date):
        """
        Add timezone information to dates
        """

        tz = pytz.timezone('UTC')
        date = tz.localize(date)
        date = datetime.datetime.astimezone(date, pytz.timezone("UTC"))
        date = date.strftime('%Y-%m-%dT%H:%M:%SZ')
        return date

    def check_xml(self, url_list, name):
        """
        Check xml to see it does not exceed 10 mb in size or 50000 urls
        """

        page_size = self.max_sitemap_size
        # get number of urls
        num_urls = len(url_list)
        size = self.get_size(url_list)

        # if number of urls is greater than max number of urls per page
        if num_urls > self.max_urls_per_page or size >= self.max_sitemap_size:

            while(page_size >= self.max_sitemap_size):
                # calculate number of pages
                num_pages = (num_urls / self.max_urls_per_page) + 1

                pages_dict_temp = {}

                # divide urls for each page starting from second page
                for page in range(2, num_pages+1):
                    p = '_'+str(page) if len(str(page)) > 1 else '_0'+str(page)
                    pages_dict_temp[name+p] =\
                        url_list[(self.max_urls_per_page*(page-1)):
                                 (self.max_urls_per_page*page)]

                    page_size = self.get_size(pages_dict_temp[name+p])

                    if page_size >= self.max_sitemap_size:
                        self.max_urls_per_page -= 10
                        break

                if page_size < self.max_sitemap_size:
                    # urls for the first page
                    pages_dict_temp[name+'_01'] =\
                        url_list[0:self.max_urls_per_page]
                    page_size = self.get_size(pages_dict_temp[name+'_01'])

        else:
            pages_dict_temp = {}
            pages_dict_temp[name+'_01'] = url_list

        return pages_dict_temp


    def get_dir_paths(self):
        #Get the User Configuration Dir Paths
        path = os.path.join( os.getcwd(),'user_config')
        if not os.path.isdir(path):
            os.makedirs(path)

        #Get the Users Artworks and Products Dir Paths
        user_artworks_path = os.path.join(path, 'user_artworks')
        user_products_path = os.path.join(path, 'user_products')
        if not os.path.isdir(user_artworks_path): os.makedirs(user_artworks_path)
        if not os.path.isdir(user_products_path): os.makedirs(user_products_path)
        return user_artworks_path, user_products_path

    #This method is made due to lack of a key value storing mechanism
    #for the namespace herewith.
    def create_config_files(self):
        """
        Creates a configuration file per User object.
        Since persistent memory is cheaper than RAM,
        stores the config file per User Object so
        it can retrive those attributes using render_list.
        Refer to scrape_db > render_list.
        This is the O(N^3) slow and scary way to create a config file.
        Don't run this all the time.

        """

        all_users_cursor = User.objects
        all_artwork_cursor = Artwork.objects
        all_products_cursor = ProductDetail.objects

        #Get the Users Artworks and Products Dir Paths
        user_artworks_path, user_products_path = self.get_dir_paths()

        for user in all_users_cursor:
            user.artwork_meta_aid_file = "user_{}_artwork_meta_aid_file.txt".format(user.uid)
            user.product_meta_pid_file = "user_{}_product_meta_aid_file.txt".format(user.uid)

            #Get Each Users Artwork and Products config file names
            each_user_artworks_file = os.path.join(user_artworks_path, \
                                                    user.artwork_meta_aid_file)
            each_user_products_file = os.path.join(user_products_path, \
                                                    user.product_meta_pid_file)
            if not os.path.isfile(each_user_artworks_file): os.makedirs(each_user_artworks_file)
            if not os.path.isfile(each_user_products_file): os.makedirs(each_user_products_file)

            for product_detail in all_products_cursor:

                for artwork in all_artwork_cursor:

                    if artwork.aid == product_detail.aid:

                        if user.uid == artwork.uid:
                            with open(each_user_artworks_file, 'wb+') as artwork_file:
                                artwork_file.write(artwork.aid)
                                artwork_file.write("\n")

                            with open(each_user_products_file, 'wb+') as product_file:
                                product_file.write(product_detail.pid)
                                product_file.write("\n")


    def scrape_db(self, pages_dict):
        """
        Generate the xml page for dynamic urls got from db
        """
        def render_list(config_file):
            '''Parses through a configuration file for a list uids and returns
            them as a list
            params:
                - config_file: A nosql attribute that stores a sequential string
                which is relative path to the file.
            '''
            with open(os.path.abspath(config_file)) as f:
                return [int(line.strip().strip(',').strip('\n')) for line in f.readlines()]


        users = []
        artworks = []
        products = []

        #TODO: please use the best/optimized/low memory method to do these
        # loops

        # Access users
        # simply use pymongo to connect and get cursor
        all_users_cursor = User.objects

        # generate user profile url
        for user in all_users_cursor:
            this_user_url = urlparse.urljoin(self.base_url, user.username)
            #TODO: there is no last mod time as of now, so use last_accessed
            this_user_lastmod = self.add_timezone(user.last_accessed)
            users.append([this_user_url, this_user_lastmod])
            #print users

            #Get the Users Artworks and Products Dir Paths
            user_artworks_path, user_products_path = self.get_dir_paths()
            #The following user_artworks,user_products works assuming
            #create_config_files() has been run once

            user_artworks = render_list(os.path.join(user_artworks_path, user.artwork_meta_aid_file))
            user_products = render_list(os.path.join(user_products_path, user.product_meta_pid_file))

            #print user_artworks
            #print user_products

            #The following coupled with the above for loop completes in O(N^2)
            all_artwork_cursor = Artwork.objects
            for aid in user_artworks:
                artwork = all_artwork_cursor.find_one({"aid": aid})
                artwork_url = urlparse.\
                            urljoin(self.base_url,
                                    user.username + '/' + artwork.slug)

                artwork_lastmod = self.add_timezone(artwork.last_modified)

                artworks.append([artwork_url, artwork_lastmod])

            pages_dict_temp = self.check_xml(artworks, 'artworks')
            pages_dict.update(pages_dict_temp)

            # all actual products
            all_products_cursor = ProductDetail.objects
            for pid in user_products:
                product_detail = all_products_cursor.find_one({"pid": pid})
                artwork = all_artwork_cursor[product_detail.find_one({"aid": aid})]
                product_url = urlparse.\
                                urljoin(self.base_url,
                                        user.username + artwork.slug +
                                        product_detail.pname)

                product_lastmod =\
                    self.add_timezone(product_detail.last_modified)

                products.append([product_url, product_lastmod])

            pages_dict_temp = self.check_xml(products, 'products')
            pages_dict.update(pages_dict_temp)


        """
        pages_dict_temp = self.check_xml(users, 'users')
        pages_dict.update(pages_dict_temp)

        # all artwork
        all_artwork_cursor = Artwork.objects

        # find the user to get profile_url, using the uid in artwork
        for artwork in all_artwork_cursor:
            for user in all_users_cursor:
                if user.uid == artwork.uid:

                    artwork_url = urlparse.\
                        urljoin(self.base_url,
                                user.username + '/' + artwork.slug)

                    artwork_lastmod = self.add_timezone(artwork.last_modified)

                    artworks.append([artwork_url, artwork_lastmod])
                    break

        pages_dict_temp = self.check_xml(artworks, 'artworks')
        pages_dict.update(pages_dict_temp)

        # all actual products
        all_products_cursor = ProductDetail.objects

        # loop, search
        for product_detail in all_products_cursor:

            for artwork in all_artwork_cursor:

                if artwork.aid == product_detail.aid:

                    for user in all_users_cursor:
                        if user.uid == artwork.uid:

                            product_url = urlparse.\
                                urljoin(self.base_url,
                                        user.username + artwork.slug +
                                        product_detail.pname)

                            product_lastmod =\
                                self.add_timezone(product_detail.last_modified)

                            products.append([product_url, product_lastmod])
                            break
                    break

        pages_dict_temp = self.check_xml(products, 'products')
        pages_dict.update(pages_dict_temp)
        """
        return pages_dict

    def run(self, verbose, dry):
        """
        Generate sitemap.xml. Makes a list of urls and date modified.
        """

        #Create the Artwork and Product Details config files per User first
        #print "Creating the Artwork and Product Details config files per User"
        #print "=" * 20
        #Run this only once
        self.create_config_files()
        #print "Artwork and Product Details config files per User created!"

        print 'Running Sitemap Generator'
        print '---' + str(datetime.datetime.utcnow()) + '---'

        start_time = time.time()

        self.setup_defaults_from_config()

        # ensure that directories exist
        if not dry:
            try:
                os.makedirs(self.sitemap_out)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    # if there was some other OSError then we raise it
                    # otherwise if directory(s) already exists we continue
                    raise

        if verbose:
            print 'Removing old sitemaps...'
        # remove all previous files
        if not dry:
            filelist = os.listdir(self.sitemap_out)
            for f in filelist:
                os.remove(self.sitemap_out + f)

        pages_dict_temp = {}

        if verbose:
            print 'Populating dynamic urls sitemap dict...'
        # dynamic
        pages_dict_temp = self.scrape_db(pages_dict_temp)
        print pages_dict_temp

        if verbose:
            print 'Generating sitemaps...'
        # generate sitemap page from template for each page
        for key in pages_dict_temp.keys():
            template = self.env.get_template('sitemap.xml')
            output_from_parsed_template = \
                template.render(pages=pages_dict_temp[key])
            if verbose:
                print 'Writing sitemap file = ' + key + ' ...'
            if dry:
                continue
            # write gzip file for each page
            with gzip.open(self.sitemap_out+key + '.xml.gz', 'wb',
                           compresslevel=9) as f:
                f.write(output_from_parsed_template)

        last_updated = self.add_timezone(datetime.datetime.utcnow())

        if verbose:
            print 'Populating index sitemap...'
        # generate sitemapindex list for all pages
        sitemaps = os.listdir(self.sitemap_out)
        sitemaps.sort()
        sitemaps = [{'loc': url_for('static', filename='sitemap/' + site,
                                    _external=True),
                     'lastmod': last_updated}
                    for site in sitemaps if not site.startswith('.')]

        # generate sitemapindex file from template
        template = self.env.get_template('sitemapindex.xml')
        output_from_parsed_template = template.render(sitemaps=sitemaps)

        if verbose:
            print 'Writing index sitemap...'
        if not dry:
            # write gzip file for sitemapindex
            with gzip.open(self.sitemap_out + 'sitemapindex.xml.gz', 'wb',
                           compresslevel=9) as f:
                f.write(output_from_parsed_template)

        print 'Done'
        print 'Time taken = ' + str(time.time() - start_time)


if __name__ == "__main__":
    manager.add_command('generate_sitemap', GenerateSitemap())
    manager.run()