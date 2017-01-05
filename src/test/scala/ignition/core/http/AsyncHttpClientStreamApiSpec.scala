package ignition.core.http

import ignition.core.http.AsyncHttpClientStreamApi.Request
import org.scalatest.{FunSpec, Matchers}

class AsyncHttpClientStreamApiSpec extends FunSpec with Matchers {

  describe(".sanitizeUrl") {
    it("should percent encode url paths") {
      val tests = Seq(
        "http://images1.petlove.com.br/products/170301/small/Ração-Special-Dog-Premium-Vegetais-Cenoura-e-Espinafre-para-Cães.jpg",
        "http://images0.petlove.com.br/products/175408/small/Ração-Nestlé-Purina-Pro-Plan-Cat-Sensitive-para-Gatos-Adultos-com-Pele-Sensível.jpg",
        "http://images3.petlove.com.br/products/171539/small/Ração-Royal-Canin-Feline-Veterinary-Diet-Urinary-SO-High-Dilution-para-Gatos-com-Cálculos-Urinários.jpg"
      )

      val expectations = Seq(
        "http://images1.petlove.com.br/products/170301/small/Ra%C3%A7%C3%A3o-Special-Dog-Premium-Vegetais-Cenoura-e-Espinafre-para-C%C3%A3es.jpg",
        "http://images0.petlove.com.br/products/175408/small/Ra%C3%A7%C3%A3o-Nestl%C3%A9-Purina-Pro-Plan-Cat-Sensitive-para-Gatos-Adultos-com-Pele-Sens%C3%ADvel.jpg",
        "http://images3.petlove.com.br/products/171539/small/Ra%C3%A7%C3%A3o-Royal-Canin-Feline-Veterinary-Diet-Urinary-SO-High-Dilution-para-Gatos-com-C%C3%A1lculos-Urin%C3%A1rios.jpg"
      )

      tests.zip(expectations).foreach {
        case (url, expected) => AsyncHttpClientStreamApi.sanitizeUrl(url).toString shouldBe expected
      }
    }

    it("should not encode percent characters in url path") {
      val url = "http://www.example.com/Pentagrama%C2%AE Acessórios em São Paulo/Qualquer%20Arquivo%20Encodado.pdf"
      val sane = AsyncHttpClientStreamApi.sanitizeUrl(url).toString
      sane shouldBe "http://www.example.com/Pentagrama%C2%AE%20Acess%C3%B3rios%20em%20S%C3%A3o%20Paulo/Qualquer%20Arquivo%20Encodado.pdf"
    }

    it("should encode space characters with percent in URL path") {
      val url = "http://www.example.com/Pentagrama+Invertido.xml?q=blah+bleh"
      val sane = AsyncHttpClientStreamApi.sanitizeUrl(url).toString
      sane shouldBe "http://www.example.com/Pentagrama%20Invertido.xml?q=blah+bleh"
    }
  }

  describe("Request") {
    it("should do the best to parse the provided uri") {
      val url = "http://www.example.com/Pentagrama%C2%AE Acessórios em São Paulo/Qualquer%20Arquivo%20Encodado.pdf"
      val request = Request(url)
      request.uri.toString shouldBe "http://www.example.com/Pentagrama%C2%AE%20Acess%C3%B3rios%20em%20S%C3%A3o%20Paulo/Qualquer%20Arquivo%20Encodado.pdf"
    }
  }
}
