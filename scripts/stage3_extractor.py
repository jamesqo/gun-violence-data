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

        return (
               *location_fields,
               *participant_fields,
                ('incident_characteristics', incident_characteristics),
                ('notes', notes),
               *guns_involved_fields,
                ('sources', sources),
               *district_fields
               )

    def _find_div_with_title(self, title, soup):
        common_parent = soup.select_one('#block-system-main')
        header = common_parent.find('h2', string=title)
        return header.parent if header else None

    def _out_name(self, in_name, prefix=''):
        return prefix + in_name.lower().replace(' ', '_') # e.g. 'Age Group' -> 'participant_age_group'

    def _getgroups(self, lines):
        groups = defaultdict(list)
        for line in lines:
            if not line:
                continue
            index = line.find(':')
            assert index != -1
            key, value = line[:index], line[index + 2:]
            groups[key].append(value)
        return groups

    def _extract_location_fields(self, soup):
        div = self._find_div_with_title('Location', soup)
        if div is None:
            return

        for span in div.select('span'):
            text = span.text
            match = re.match(r'^Geolocation: (.*), (.*)$', text)
            if match:
                latitude, longitude = float(match.group(1)), float(match.group(2))
                yield 'latitude', latitude
                yield 'longitude', longitude
            elif re.match(r'^(.*), (.*)$', text) or re.match(r'^[0-9]+ ', text):
                # Nothing to be done. City, state, and address fields are already included in the stage2 dataset.
                pass
            else:
                yield 'location_description', text

    def _extract_participant_fields(self, soup):
        div = self._find_div_with_title('Participants', soup)
        if div is None:
            return

        lines = [li.text for li in div.select('li')]
        for field_name, field_values in self._getgroups(lines).items():
            field_name = self._out_name(field_name, prefix='participant_')
            # TODO: Ensure that 'values', which is a list, can be serialized properly by DataFrame.to_csv().
            yield field_name, field_values

    def _extract_incident_characteristics(self, soup):
        div = self._find_div_with_title('Incident Characteristics', soup)
        return [] if div is None else [li.text for li in div.select('li')]

    def _extract_notes(self, soup):
        div = self._find_div_with_title('Notes', soup)
        return '' if div is None else div.select_one('p').text

    def _extract_guns_involved_fields(self, soup):
        div = self._find_div_with_title('Guns Involved', soup)
        if div is None:
            return

        # n_guns_involved
        p_text = div.select_one('p').text
        match = re.match(r'^([0-9]+) guns? involved.$', p_text)
        assert match, "<p> text did not match expected pattern: {}".format(p_text)
        n_guns_involved = int(match.group(1))
        yield 'n_guns_involved', n_guns_involved

        # List attributes
        lines = [li.text for li in div.select('li')]
        for field_name, field_values in self._getgroups(lines).items():
            field_name = self._out_name(field_name, prefix='gun_')
            # TODO: Ensure that 'values', which is a list, can be serialized properly by DataFrame.to_csv().
            yield field_name, field_values

    def _extract_sources(self, soup):
        # TODO
        return
        yield

    def _extract_district_fields(self, soup):
        div = self._find_div_with_title('District', soup)
        if div is None:
            return

        # The text we want to scrape is orphaned (no direct parent element), so we can't get at it directly.
        # Fortunately, each important line is followed by a <br> element, so we can use that to our advantage.
        # NB: The orphaned text elements are of type 'NavigableString'
        lines = [str(br.previousSibling).strip() for br in div.select('br')]
        for key, values in self._getgroups(lines).items():
            assert len(values) == 1 # It would be strange if the incident took place in more than 1 congressional district
            yield self._out_name(key), values[0]
