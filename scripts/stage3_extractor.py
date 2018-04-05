from bs4 import BeautifulSoup

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
                incident_characteristics,
                notes,
               *guns_involved_fields,
                sources,
               *district_fields
