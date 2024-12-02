def test_CustomPageIncrement(custom_page_increment):
  my_component = custom_page_increment
  my_component.reset()
  assert my_component._page == 1
