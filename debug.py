class MetaTagCollector(type):
    """Метакласс для создания единственного экземпляра класса TagCollector."""

    _instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance
    
class TagCollector(metaclass=MetaTagCollector):
    """Класс для сбора тегов из списка лидов."""
    _raw_tags = []

    def add_tag(self, tag: str):
        """Добавляет тег в список тегов."""
        self._raw_tags.append(tag)
    
    def show_all_tags(self):
        """Показывает все теги."""
        return self._raw_tags

if __name__ == "__main__":

    with open('tags.txt', 'r', encoding='utf-8') as file:
        tags = set(file.readlines())
        tags = [tag.strip() for tag in tags]
        with open('tags_set.txt', 'w', encoding='utf-8') as file_set:
            file_set.write('\n'.join(tags))