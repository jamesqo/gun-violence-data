import re

from bs4 import BeautifulSoup
from collections import defaultdict

class Stage3Extractor(object):
    def extract_fields(self, text):
        soup = BeautifulSoup(text, features='html5lib')

        location_fields = self._extract_location_fields(soup)
        participant_fields = self._extract_participant_fields(soup)
        incident_characteristics = self._extract_incident_characteristics(soup)
        notes = self._extract_notes(soup)
        guns_involved_fields = self._extract_guns_involved_fields(soup)
        sources = self._extract_sources(soup)
        district_fields = self._extract_district_fields(soup)

        return *location_fields,
               *participant_fields,
                ('incident_characteristics', incident_characteristics),
                ('notes', notes),
               *guns_involved_fields,
                ('sources', sources),
               *district_fields

    def _find_div_with_title(self, title, soup):
        common_parent = soup.select_one('#block-system-main')
        header = common_parent.find('h2', string=title)
        return header.parent if header else None

    def _extract_location_fields(self, soup):
        div = self._find_div_with_title('Location', soup)
        if div is None:
            return

        for span in div.select('span'):
            text = span.text
            match = re.match(r'^Geolocation: (.*), (.*)$', text):
            if match:
                latitude, longitude = float(match.group(1)), float(match.group(2))
                yield ('latitude', latitude)
                yield ('longitude', longitude)
            elif re.match(r'^(.*), (.*)$', text) or re.match(r'^[0-9]+ ', text):
                # Nothing to be done. City, state, and address fields are already included in the stage2 dataset.
                pass
            else:
                yield ('location_description', text)

    def _extract_participant_fields(self, soup):
        def out_name(in_name):
            # Example: in_name = 'Age Group' -> out_name = 'participant_age_group'
            return 'participant_' + in_name.lower().replace(' ', '_')

        div = self._find_div_with_title('Participants', soup)
        if div is None:
            return []

        fields = defaultdict(list)
        for ul in div.select('ul'):
            for li in ul.children:
                text = li.text
                key, value = text[:text.find(':')], text[text.find(':') + 2:]
                fields[key].append(value)
        # TODO: Ensure that 'values', which is a list, can be serialized properly by DataFrame.to_csv().
        return [(out_name(key), values) for key, values in fields.items()]

    def _extract_incident_characteristics(self, soup):
        div = self._find_div_with_title('Incident Characteristics', soup)
        return [] if div is None else [li.text for li in div.select('ul li')]

    def _extract_notes(self, soup):
        div = self._find_div_with_title('Notes', soup)
        return '' if div is None else div.select_one('p').text

    def _extract_guns_involved_fields(self, soup):
        def out_name(in_name):
            return 'gun_' + in_name.lower()

        div = self._find_div_with_title('Guns Involved', soup)
        if div is None:
            return

        p_text = div.select_one('p').text
        match = re.match(r'^([0-9]+) guns involved.$')
        assert match, "'{}' did not match expected pattern".format(p_text)
        n_guns_involved = int(match.group(1))
        yield ('n_guns_involved', n_guns_involved)

        fields = defaultdict(list)
        for ul in div.select('ul'):
            for li in ul.children:
                text = li.text
                key, value = text[:text.find(':')], text[text.find(':') + 2:]
                fields[key].append(value)
        # TODO: Ensure that 'values', which is a list, can be serialized properly by DataFrame.to_csv().
        return [(out_name(key), values) for key, values in fields.items()]

    def _extract_sources(self, soup):
        # TODO
        return []

    def _extract_district_fields(self, soup):
        def out_name(in_name):
            return in_name.lower().replace(' ', '_')

        div = self._find_div_with_title('District', soup)
        if div is None:
            return

        for br in div.select('br'):
            text = br.previousSibling.text
            key, value = text[:text.find(':')], text[text.find(':') + 2:]
            yield (out_name(key), int(value))

